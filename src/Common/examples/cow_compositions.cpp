#include <Common/COW.h>
#include <iostream>


class IColumn : public COW<IColumn>
{
private:
    friend class COW<IColumn>;

    virtual MutablePtr clone() const = 0;
    virtual MutablePtr deepMutate() const { return shallowMutate(); }

public:
    IColumn() = default;
    IColumn(const IColumn &) = default;
    virtual ~IColumn() = default;

    virtual int get() const = 0;
    virtual void set(int value) = 0;

    static MutablePtr mutate(Ptr ptr) { return ptr->deepMutate(); }
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

class ColumnComposition : public COWHelper<IColumn, ColumnComposition>
{
private:
    friend class COWHelper<IColumn, ColumnComposition>;

    ConcreteColumn::WrappedPtr wrapped;

    explicit ColumnComposition(int data) : wrapped(ConcreteColumn::create(data)) {}
    ColumnComposition(const ColumnComposition &) = default;

    IColumn::MutablePtr deepMutate() const override
    {
        std::cerr << "Mutating\n";
        auto res = shallowMutate();
        res->wrapped = IColumn::mutate(std::move(res->wrapped).detach());
        return res;
    }

public:
    int get() const override { return wrapped->get(); }
    void set(int value) override { wrapped->set(value); }
};


int main(int, char **)
{
    ColumnPtr x = ColumnComposition::create(1);
    ColumnPtr y = x;

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << x.get() << ", " << y.get() << "\n";

    {
        MutableColumnPtr mut = IColumn::mutate(std::move(y));
        mut->set(2);

        std::cerr << "refcounts: " << x->use_count() << ", " << mut->use_count() << "\n";
        std::cerr << "addresses: " << x.get() << ", " << mut.get() << "\n";
        y = std::move(mut);
    }

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << x.get() << ", " << y.get() << "\n";

    x = ColumnComposition::create(0);

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";
    std::cerr << "addresses: " << x.get() << ", " << y.get() << "\n";

    {
        MutableColumnPtr mut = IColumn::mutate(std::move(y));
        mut->set(3);

        std::cerr << "refcounts: " << x->use_count() << ", " << mut->use_count() << "\n";
        std::cerr << "addresses: " << x.get() << ", " << mut.get() << "\n";
        y = std::move(mut);
    }

    std::cerr << "values:    " << x->get() << ", " << y->get() << "\n";
    std::cerr << "refcounts: " << x->use_count() << ", " << y->use_count() << "\n";

    return 0;
}

