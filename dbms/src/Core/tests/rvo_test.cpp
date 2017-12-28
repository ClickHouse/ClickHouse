#include <iostream>
#include <stdlib.h>
#include <vector>
#include <memory>


struct C
{
    volatile int data;

    explicit C(int n = 0) : data(n) {}

    C(const C & x)
    {
        *this = x;
    }

    C & operator= (const C & x)
    {
        std::cerr << "copy ";
        data = x.data;
        return *this;
    }

    void clear() { data = 0; }
    void swap(C & other) { std::swap(data, other.data); }
    C & ref() { return *this; }
};


C f1()
{
    return C(1);
}

C f2()
{
    C x;
    ++x.data;

    return x;
}

C f3()
{
    if (rand() % 10 == 0)
        return C(123);

    C x;
    ++x.data;

    if (rand() % 10 == 0)
        return C(456);

    return x;
}

C f4()
{
    if (rand() % 10 == 0)
        return C(123);

    C x;
    ++x.data;

    return x;
}

C f5()
{
    C x;
    ++x.data;

    if (rand() % 10 == 0)
        return C(123);

    return x;
}

C f6()
{
    C x;
    ++x.data;

    if (rand() % 10 == 0)
    {
        x.data = 123;
        return x;
    }

    return x;
}

C f7()
{
    C x;
    ++x.data;

    if (rand() % 10 == 0)
        x.data = 123;

    return x;
}

C f8()
{
    C x;
    ++x.data;

    if (rand() % 2)
        x = f2();

    return x;
}

C f9()
{
    C x;
    ++x.data;

    if (rand() % 2)
        x = f9();

    return x;
}

C f10()
{
    C x;
    ++x.data;

    if (rand() % 2)
    {
        C new_x = f2();
        x.data = new_x.data;
    }

    return x;
}

C f11()
{
    return (rand() % 2) ? f1() : f2();
}

C f12()
{
    if (rand() % 2)
        return f1();
    else
        return f2();
}

C f13()
{
    C x;

    if (rand() % 2)
        x = f1();
    else
        x = f2();

    return x;
}

C f14()
{
    C x;

    if (rand() % 2)
        x.data = f1().data;
    else
        x.data = f2().data;

    return x;
}

C f15()
{
    C x;

    if (rand() % 2)
        return x;

    x = f1();

    if (rand() % 2)
    {
        x.clear();
        return x;
    }

    return x;
}

C f16()
{
    C x;

    if (rand() % 2)
        return x;

    x.swap(f1().ref());

    if (rand() % 2)
    {
        x.clear();
        return x;
    }

    return x;
}


struct IFactory
{
    virtual C get() const = 0;
    virtual ~IFactory() {}
};

using FactoryPtr = std::unique_ptr<IFactory>;


struct Factory1 : IFactory
{
    C get() const { return f1(); }
};

struct Factory2 : IFactory
{
    C get() const { return f2(); }
};

struct Factory3 : IFactory
{
    C get() const
    {
        FactoryPtr factory;

        if (rand() % 2)
            factory = FactoryPtr(new Factory1);
        else
            factory = FactoryPtr(new Factory2);

        return factory->get();
    }
};


int main(int, char **)
{
    srand(time(nullptr));

    std::cerr << "f1: " << f1().data << std::endl;
    std::cerr << "f2: " << f2().data << std::endl;
    std::cerr << "f3: " << f3().data << std::endl;
    std::cerr << "f4: " << f4().data << std::endl;
    std::cerr << "f5: " << f5().data << std::endl;
    std::cerr << "f6: " << f6().data << std::endl;
    std::cerr << "f7: " << f7().data << std::endl;
    std::cerr << "f8: " << f8().data << std::endl;
    std::cerr << "f9: " << f9().data << std::endl;
    std::cerr << "f10: " << f10().data << std::endl;
    std::cerr << "f11: " << f11().data << std::endl;
    std::cerr << "f12: " << f12().data << std::endl;
    std::cerr << "f13: " << f13().data << std::endl;
    std::cerr << "f14: " << f14().data << std::endl;
    std::cerr << "f15: " << f15().data << std::endl;
    std::cerr << "f16: " << f16().data << std::endl;

    using Factories = std::vector<FactoryPtr>;
    Factories factories;
    factories.push_back(FactoryPtr(new Factory1));
    factories.push_back(FactoryPtr(new Factory2));
    factories.push_back(FactoryPtr(new Factory3));

    for (size_t i = 0; i < factories.size(); ++i)
        std::cerr << "Factory" << (i + 1) << ": " << factories[i]->get().data << std::endl;

    return 0;
}
