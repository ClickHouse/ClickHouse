#include <Common/COWPtr.h>
#include <iostream>


class Column : public COWPtr<Column>
{
private:
    friend class COWPtr<Column>;

    int data;
    Column(int data) : data(data) {}

public:
    int get() const { return data; }
    void set(int value) { data = value; }
};

using ColumnPtr = Column::Ptr;
using MutableColumnPtr = Column::MutablePtr;


int main(int, char **)
{
    ColumnPtr x = Column::create(1);
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

    x = Column::create(0);

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

