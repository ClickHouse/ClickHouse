#include <Common/COW.h>
#include <iostream>


class IColumn : public COW<IColumn>
{
private:
    friend class COW<IColumn>;
    virtual MutablePtr clone() const = 0;

public:
    IColumn() = default;
    IColumn(const IColumn &) = default;
    virtual ~IColumn() = default;

    virtual int get() const = 0;
    virtual void set(int value) = 0;

    virtual MutablePtr test() const = 0;
};

using ColumnPtr = IColumn::Ptr;
using MutableColumnPtr = IColumn::MutablePtr;

class ConcreteColumn : public COWHelper<IColumn, ConcreteColumn>
{
private:
    friend class COWHelper<IColumn, ConcreteColumn>;

    int data;
    ConcreteColumn(int data) : data(data) {}
    ConcreteColumn(const ConcreteColumn &) = default;

    MutableColumnPtr test() const override
    {
        MutableColumnPtr res = create(123);
        return res;
    }

public:
    int get() const override { return data; }
    void set(int value) override { data = value; }
};


int main(int, char **)
{
    ColumnPtr x = ConcreteColumn::create(1);
    ColumnPtr y = x;//x->test();

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << x.get() << ", " << y.get() << "\n";

    {
        MutableColumnPtr mut = std::move(*y).mutate();
        mut->set(2);

        std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << ", " << mut->use_count() << "\n";
        std::cerr << "addresses: " << x.get() << ", " << y.get() << ", " << mut.get() << "\n";
        y = std::move(mut);
    }

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << x.get() << ", " << y.get() << "\n";

    x = ConcreteColumn::create(0);

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << x.get() << ", " << y.get() << "\n";

    {
        MutableColumnPtr mut = std::move(*y).mutate();
        mut->set(3);

        std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << ", " << mut->use_count() << "\n";
        std::cerr << "addresses: " << x.get() << ", " << y.get() << ", " << mut.get() << "\n";
        y = std::move(mut);
    }

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";

    return 0;
}

