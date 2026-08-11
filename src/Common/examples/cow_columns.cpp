#include <Common/COW.h>
#include <iostream>
#include <base/defines.h>


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
};

using ColumnPtr = IColumn::Ptr;
using MutableColumnPtr = IColumn::MutablePtr;

class ConcreteColumn : public COWHelper<IColumn, ConcreteColumn>
{
private:
    friend class COWHelper<IColumn, ConcreteColumn>;

    int data;
    explicit ConcreteColumn(int data_) : data(data_) {}
    ConcreteColumn(const ConcreteColumn &) = default;

public:
    int get() const override { return data; }
    void set(int value) override { data = value; }
};

template <typename ColPtr>
void print(const ColumnPtr & x, const ColPtr & y)
{
    std::cerr << "values:    " << x->get()        << ", " << y->get()       << "\n";
    std::cerr << "refcounts: " << x->use_count()  << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << x.get()         << ", " << y.get()        << "\n";
}

int main(int, char **)
{
    ColumnPtr x = ConcreteColumn::create(1);
    ColumnPtr y = x;
    print(x, y);
    chassert(x->get() == 1 && y->get() == 1);
    chassert(x->use_count() == 2 && y->use_count() == 2);
    chassert(x.get() == y.get());

    {
        MutableColumnPtr mut = IColumn::mutate(std::move(y));
        mut->set(2);
        print(x, mut);
        chassert(x->get() == 1 && mut->get() == 2);
        chassert(x->use_count() == 1 && mut->use_count() == 1);
        chassert(x.get() != mut.get());

        y = std::move(mut);
    }
    print(x, y);
    chassert(x->get() == 1 && y->get() == 2);
    chassert(x->use_count() == 1 && y->use_count() == 1);
    chassert(x.get() != y.get());

    x = ConcreteColumn::create(0);
    print(x, y);
    chassert(x->get() == 0 && y->get() == 2);
    chassert(x->use_count() == 1 && y->use_count() == 1);
    chassert(x.get() != y.get());

    {
        MutableColumnPtr mut = IColumn::mutate(std::move(y));
        mut->set(3);
        print(x, mut);
        chassert(x->get() == 0 && mut->get() == 3);
        chassert(x->use_count() == 1 && mut->use_count() == 1);
        chassert(x.get() != mut.get());

        y = std::move(mut);
    }
    print(x, y);
    chassert(x->get() == 0 && y->get() == 3);
    chassert(x->use_count() == 1 && y->use_count() == 1);
    chassert(x.get() != y.get());

    return 0;
}
