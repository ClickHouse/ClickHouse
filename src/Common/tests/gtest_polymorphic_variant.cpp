#include <variant>

#include <Common/PolymorhicVariantFactory.h>

#include <gtest/gtest.h>


// Base polymorphic class
struct Base {
    virtual std::string name() = 0;
    virtual ~Base() = default;
};

struct A: Base {
    std::string name() override { return "A"; }
};
struct A1: A
{
    std::string name() override { return "A1"; }
};
struct A2: A {
    std::string name() override { return "A2"; }
};
using ATypes = TypeList<A1, A2>;

struct B: Base {
    std::string name() override { return "B"; }
};
struct B1: B {
    std::string name() override { return "B1"; }
};
struct B2: B {
    std::string name() override { return "B2"; }
};
using BTypes = TypeList<B1, B2>;


template <typename Types, typename T>
concept CanCreateFrom = requires(T&& t)
{
    PolymorhicVariantFactory<Types>::AsVariant(std::forward<T>(t));
};


TEST(PolymorphicVariant, Create)
{
    // pointer
    {
        A1 a;
        auto v = PolymorhicVariantFactory<ATypes>::AsVariant(static_cast<Base*>(&a));

        ::testing::StaticAssertTypeEq<decltype(v), std::variant<A1*, A2*>>();
    }

    // pointer to const
    {
        const A1 a;
        auto v = PolymorhicVariantFactory<ATypes>::AsVariant(static_cast<const Base*>(&a));

        ::testing::StaticAssertTypeEq<decltype(v), std::variant<const A1*, const A2*>>();
    }

    // lvalue
    {
        A1 a;
        auto v = PolymorhicVariantFactory<ATypes>::AsVariant(a);

        ::testing::StaticAssertTypeEq<decltype(v), std::variant<A1*, A2*>>();
    }

    // const lvalue
    {
        const A1 a;
        auto v = PolymorhicVariantFactory<ATypes>::AsVariant(a);

        ::testing::StaticAssertTypeEq<decltype(v), std::variant<const A1*, const A2*>>();
    }

    // lvalue ref
    {
        const A1 a;
        auto v = PolymorhicVariantFactory<ATypes>::AsVariant(static_cast<const Base&>(a));

        ::testing::StaticAssertTypeEq<decltype(v), std::variant<const A1*, const A2*>>();
    }

    // rvalue
    {
        static_assert(!CanCreateFrom<ATypes, A1&&>);
    }
}

TEST(PolymorphicVariant, VisitSimple)
{
    A1 a;
    auto v = PolymorhicVariantFactory<ATypes>::AsVariant(a);

    EXPECT_EQ(std::visit([](Base* b){return b->name();}, v), "A1");
    EXPECT_EQ(std::visit([](auto* b){return b->name();}, v), "A1");
}

TEST(PolymorphicVariant, VisitOverload)
{
    auto visitor = Overloaded{
        [](A1* b){return b->name();},
        [](A2* b){return b->name();},
    };

    A1 a1;
    auto v = PolymorhicVariantFactory<ATypes>::AsVariant(a1);
    EXPECT_EQ(std::visit(visitor, v), "A1");

    A2 a2;
    v = PolymorhicVariantFactory<ATypes>::AsVariant(a2);    
    EXPECT_EQ(std::visit(visitor, v), "A2");
}

TEST(PolymorphicVariant, VisitOverloadPairwise)
{
    auto visitor = Overloaded{
        [](A1* a, B1* b){return a->name() + " " + b->name() + " overload";},
        [](auto* a, auto* b){return a->name() + " " + b->name() + " auto fallback";},
    };

    A1 a1;
    A2 a2;
    B1 b1;
    B2 b2;

    auto v_a = PolymorhicVariantFactory<ATypes>::AsVariant(static_cast<Base*>(&a1));
    auto v_b = PolymorhicVariantFactory<BTypes>::AsVariant(static_cast<Base*>(&b1));

    EXPECT_EQ(std::visit(visitor, v_a, v_b), "A1 B1 overload");

    v_a = &a2;
    v_b = &b2;
    
    EXPECT_EQ(std::visit(visitor, v_a, v_b), "A2 B2 auto fallback");
}

TEST(PolymorphicVariant, VisitOverloadImplicitCastToBase)
{
    auto visitor = Overloaded{
        [](A1* a, B1* b){return a->name() + " " + b->name() + " exact";},
        [](A* a, B* b){return a->name() + " " + b->name() + " implicit";},
    };

    A1 a1;
    A2 a2;
    B1 b1;
    B2 b2;

    auto v_a = PolymorhicVariantFactory<ATypes>::AsVariant(static_cast<Base*>(&a1));
    auto v_b = PolymorhicVariantFactory<BTypes>::AsVariant(static_cast<Base*>(&b1));

    EXPECT_EQ(std::visit(visitor, v_a, v_b), "A1 B1 exact");

    v_a = &a2;
    v_b = &b2;
    
    EXPECT_EQ(std::visit(visitor, v_a, v_b), "A2 B2 implicit");
}
