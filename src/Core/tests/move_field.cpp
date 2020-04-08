#include <iostream>
#include <Core/Field.h>


int main(int, char **)
{
    using namespace DB;

    Field f;

    f = Field{String{"Hello, world"}};
    std::cerr << f.get<String>() << "\n";
    f = Field{String{"Hello, world!"}};
    std::cerr << f.get<String>() << "\n";
    f = Field{Array{Field{String{"Hello, world!!"}}}};
    std::cerr << f.get<Array>()[0].get<String>() << "\n";
    f = String{"Hello, world!!!"};
    std::cerr << f.get<String>() << "\n";
    f = Array{Field{String{"Hello, world!!!!"}}};
    std::cerr << f.get<Array>()[0].get<String>() << "\n";
    f = Array{String{"Hello, world!!!!!"}};
    std::cerr << f.get<Array>()[0].get<String>() << "\n";

    return 0;
}
