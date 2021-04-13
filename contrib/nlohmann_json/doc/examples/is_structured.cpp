#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

int main()
{
    // create JSON values
    json j_null;
    json j_boolean = true;
    json j_number_integer = 17;
    json j_number_float = 23.42;
    json j_number_unsigned_integer = 12345678987654321u;
    json j_object = {{"one", 1}, {"two", 2}};
    json j_array = {1, 2, 4, 8, 16};
    json j_string = "Hello, world";
    json j_binary = json::binary({1, 2, 3});

    // call is_structured()
    std::cout << std::boolalpha;
    std::cout << j_null.is_structured() << '\n';
    std::cout << j_boolean.is_structured() << '\n';
    std::cout << j_number_integer.is_structured() << '\n';
    std::cout << j_number_unsigned_integer.is_structured() << '\n';
    std::cout << j_number_float.is_structured() << '\n';
    std::cout << j_object.is_structured() << '\n';
    std::cout << j_array.is_structured() << '\n';
    std::cout << j_string.is_structured() << '\n';
    std::cout << j_binary.is_structured() << '\n';
}
