#include <gtest/gtest.h>
#include <Parsers/IAST.h>
#include <Common/StackTrace.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
    size_t chain_lenght = 10;

    {
        DB::ASTPtr ast = std::make_shared<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);

        for (size_t i = 0; i < chain_lenght; ++i)
            ast = std::make_shared<ASTWithDecrementer>(DB::ASTs{std::move(ast)}, depth, max_depth);
    }
}

TEST(ASTDeleter, SimpleChainLong)
{
    size_t depth = 0;
    size_t max_depth = 5;
    size_t chain_lenght = 100000;

    {
        DB::ASTPtr ast = std::make_shared<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);

        for (size_t i = 0; i < chain_lenght; ++i)
            ast = std::make_shared<ASTWithDecrementer>(DB::ASTs{std::move(ast)}, depth, max_depth);
    }
}

TEST(ASTDeleter, ChainWithExtraMember)
{
    size_t depth = 0;
    size_t max_depth = 5;
    size_t member_depth = 10;
    size_t chain_lenght = 100;

    {
        DB::ASTPtr ast = std::make_shared<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);

        for (size_t i = 0; i < chain_lenght; ++i)
        {
            ast = std::make_shared<ASTWithDecrementer>(DB::ASTs{std::move(ast)}, depth, max_depth);
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
    size_t chain_lenght = 10;

    {
        DB::ASTPtr ast1 = std::make_shared<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);
        DB::ASTPtr ast2 = std::make_shared<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);

        for (size_t i = 0; i < chain_lenght; ++i)
        {
            ast1 = std::make_shared<ASTWithDecrementer>(DB::ASTs{std::move(ast1)}, depth, max_depth);
            ast2 = std::make_shared<ASTWithDecrementer>(DB::ASTs{std::move(ast2)}, depth, max_depth);
            ast1->as<ASTWithDecrementer>()->members.push_back(ast2->children.front());
            ast2->as<ASTWithDecrementer>()->members.push_back(ast1->children.front());
        }
    }
}

TEST(ASTDeleter, DoubleChainLong)
{
    size_t depth = 0;
    size_t max_depth = 5;
    size_t chain_lenght = 100000;

    {
        DB::ASTPtr ast1 = std::make_shared<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);
        DB::ASTPtr ast2 = std::make_shared<ASTWithDecrementer>(DB::ASTs{}, depth, max_depth);

        for (size_t i = 0; i < chain_lenght; ++i)
        {
            ast1 = std::make_shared<ASTWithDecrementer>(DB::ASTs{std::move(ast1)}, depth, max_depth);
            ast2 = std::make_shared<ASTWithDecrementer>(DB::ASTs{std::move(ast2)}, depth, max_depth);
            ast1->as<ASTWithDecrementer>()->members.push_back(ast2->children.front());
            ast2->as<ASTWithDecrementer>()->members.push_back(ast1->children.front());
        }
    }
}
