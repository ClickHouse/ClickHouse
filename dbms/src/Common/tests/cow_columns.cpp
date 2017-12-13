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
};

class ConcreteColumn : public COWPtrHelper<IColumn, ConcreteColumn>
{
private:
    friend class COWPtrHelper<IColumn, ConcreteColumn>;

    int data;
    ConcreteColumn(int data) : data(data) {}
    ConcreteColumn(const ConcreteColumn &) = default;

public:
    int get() const override { return data; }
    void set(int value) override { data = value; }
};

using ColumnPtr = IColumn::Ptr;
using MutableColumnPtr = IColumn::MutablePtr;


int main(int, char **)
{
    ColumnPtr x = ConcreteColumn::create(1);
    ColumnPtr y = x;

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << &*x << ", " << &*y << "\n";

    {
        MutableColumnPtr mutable_y = y->mutate();
        mutable_y->set(2);

        std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << ", " << mutable_y->use_count() << "\n";
        std::cerr << "addresses: " << &*x << ", " << &*y << ", " << &*mutable_y << "\n";
        y = std::move(mutable_y);
    }

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << &*x << ", " << &*y << "\n";

    x = ConcreteColumn::create(0);

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << &*x << ", " << &*y << "\n";

    {
        MutableColumnPtr mutable_y = y->mutate();
        mutable_y->set(3);

        std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << ", " << mutable_y->use_count() << "\n";
        std::cerr << "addresses: " << &*x << ", " << &*y << ", " << &*mutable_y << "\n";
        y = std::move(mutable_y);
    }

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";

    return 0;
}

