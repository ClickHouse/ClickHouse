#include <Common/COWPtr.h>
#include <iostream>


class IColumn : public COWPtr<IColumn>
{
private:
    friend class COWPtr<IColumn>;
    virtual IColumn * clone() const = 0;

public:
    virtual ~IColumn() {}

    virtual int get() const = 0;
    virtual void set(int value) = 0;

    virtual MutablePtr test() const = 0;
};

using ColumnPtr = IColumn::Ptr;
using MutableColumnPtr = IColumn::MutablePtr;

class ConcreteColumn : public COWPtrHelper<IColumn, ConcreteColumn>
{
private:
    friend class COWPtrHelper<IColumn, ConcreteColumn>;

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
    ColumnPtr y = x->test();

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << x.get() << ", " << y.get() << "\n";

    {
        MutableColumnPtr mutable_y = y->mutate();
        mutable_y->set(2);

        std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << ", " << mutable_y->use_count() << "\n";
        std::cerr << "addresses: " << x.get() << ", " << y.get() << ", " << mutable_y.get() << "\n";
        y = std::move(mutable_y);
    }

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << x.get() << ", " << y.get() << "\n";

    x = ConcreteColumn::create(0);

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << x.get() << ", " << y.get() << "\n";

    {
        MutableColumnPtr mutable_y = y->mutate();
        mutable_y->set(3);

        std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << ", " << mutable_y->use_count() << "\n";
        std::cerr << "addresses: " << x.get() << ", " << y.get() << ", " << mutable_y.get() << "\n";
        y = std::move(mutable_y);
    }

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";

    return 0;
}

