#include <gtest/gtest.h>
#include <Parsers/IAST.h>
#include <Common/StackTrace.h>

struct StackDecrementer
{
    size_t & depth;

    explicit StackDecrementer(size_t & depth_) : depth(depth_) {}

    virtual ~StackDecrementer()
    {
        --depth;
    }
};

struct ASTCounting : public StackDecrementer, public DB::IAST
{
    explicit ASTCounting(size_t & depth_) : StackDecrementer(depth_) {}

    String getID(char) const override { return ""; }
    DB::ASTPtr clone() const override { return nullptr; }
};

class ASTWithMembers : public ASTCounting
{
public:
    explicit ASTWithMembers(DB::ASTs members_, size_t & depth_) : ASTCounting(depth_), members(std::move(members_))
    {
        children = members;
    }

    DB::ASTs members;
};

class ASTWithDecrementer : public ASTWithMembers
{
public:
    ASTWithDecrementer(DB::ASTs members_, size_t & depth_, size_t max_depth_)
        : ASTWithMembers(std::move(members_), depth_), max_depth(max_depth_)
    {
    }

    size_t max_depth;

    ~ASTWithDecrementer() override
    {
        ++depth;
        if (depth == max_depth)
            std::cout << StackTrace().toString() << std::endl;
        if (depth > max_depth)
            EXPECT_LE(depth, max_depth);
    }
};


TEST(ASTDeleter, SimpleChain)
{
    size_t depth = 0;
    size_t max_depth = 5;
    size_t chain_length = 10;

    {
        DB::ASTPtr ast = make_intrusive<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);

        for (size_t i = 0; i < chain_length; ++i)
            ast = make_intrusive<ASTWithDecrementer>(DB::ASTs{std::move(ast)}, depth, max_depth);
    }
}

TEST(ASTDeleter, SimpleChainLong)
{
    size_t depth = 0;
    size_t max_depth = 5;
    size_t chain_length = 100000;

    {
        DB::ASTPtr ast = make_intrusive<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);

        for (size_t i = 0; i < chain_length; ++i)
            ast = make_intrusive<ASTWithDecrementer>(DB::ASTs{std::move(ast)}, depth, max_depth);
    }
}

TEST(ASTDeleter, ChainWithExtraMember)
{
    size_t depth = 0;
    size_t max_depth = 5;
    size_t member_depth = 10;
    size_t chain_length = 100;

    {
        DB::ASTPtr ast = make_intrusive<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);

        for (size_t i = 0; i < chain_length; ++i)
        {
            ast = make_intrusive<ASTWithDecrementer>(DB::ASTs{std::move(ast)}, depth, max_depth);
            if (i > member_depth)
            {
                DB::ASTPtr member = ast;
                for (size_t j = 0; j < member_depth; ++j)
                    member = member->children.front();
                ast->as<ASTWithDecrementer>()->members.push_back(std::move(member));
            }
        }
    }
}

TEST(ASTDeleter, DoubleChain)
{
    size_t depth = 0;
    size_t max_depth = 5;
    size_t chain_length = 10;

    {
        DB::ASTPtr ast1 = make_intrusive<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);
        DB::ASTPtr ast2 = make_intrusive<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);

        for (size_t i = 0; i < chain_length; ++i)
        {
            ast1 = make_intrusive<ASTWithDecrementer>(DB::ASTs{std::move(ast1)}, depth, max_depth);
            ast2 = make_intrusive<ASTWithDecrementer>(DB::ASTs{std::move(ast2)}, depth, max_depth);
            ast1->as<ASTWithDecrementer>()->members.push_back(ast2->children.front());
            ast2->as<ASTWithDecrementer>()->members.push_back(ast1->children.front());
        }
    }
}

TEST(ASTDeleter, DoubleChainLong)
{
    size_t depth = 0;
    size_t max_depth = 5;
    size_t chain_length = 100000;

    {
        DB::ASTPtr ast1 = make_intrusive<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);
        DB::ASTPtr ast2 = make_intrusive<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);

        for (size_t i = 0; i < chain_length; ++i)
        {
            ast1 = make_intrusive<ASTWithDecrementer>(DB::ASTs{std::move(ast1)}, depth, max_depth);
            ast2 = make_intrusive<ASTWithDecrementer>(DB::ASTs{std::move(ast2)}, depth, max_depth);
            ast1->as<ASTWithDecrementer>()->members.push_back(ast2->children.front());
            ast2->as<ASTWithDecrementer>()->members.push_back(ast1->children.front());
        }
    }
}

namespace
{

class ASTDestructionObserver : public DB::IAST
{
public:
    explicit ASTDestructionObserver(size_t & observations_) : observations(observations_) {}

    String getID(char) const override { return "ASTDestructionObserver"; }
    DB::ASTPtr clone() const override { return nullptr; }

    ~ASTDestructionObserver() override
    {
        // dumpTree observes virtual dispatch while this intermediate base is still alive.
        EXPECT_TRUE(dumpTree().starts_with("ASTDestructionObserver\n"));
        ++observations;
    }

private:
    size_t & observations;
};

class ASTSecondaryBase : public StackDecrementer, public ASTDestructionObserver
{
public:
    ASTSecondaryBase(size_t & alive_, size_t & observations_)
        : StackDecrementer(alive_), ASTDestructionObserver(observations_)
    {
    }

    String getID(char) const override { return "ASTSecondaryBase"; }
};

}

TEST(ASTDeleter, SecondaryBaseWithChild)
{
    size_t parents_alive = 1;
    size_t children_alive = 1;
    size_t observations = 0;
    DB::ASTPtr parent = DB::make_intrusive<ASTSecondaryBase>(parents_alive, observations);
    ASSERT_NE(static_cast<void *>(parent.get()), dynamic_cast<void *>(parent.get()));
    EXPECT_TRUE(parent->dumpTree().starts_with("ASTSecondaryBase\n"));

    parent->children.push_back(DB::make_intrusive<ASTCounting>(children_alive));
    ASSERT_EQ(parent->use_count(), 1);
    ASSERT_EQ(parent->children.front()->use_count(), 1);

    parent.reset();

    EXPECT_EQ(observations, 1);
    EXPECT_EQ(parents_alive, 0);
    EXPECT_EQ(children_alive, 0);
}
