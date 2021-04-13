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
    json j_object = {{"one", 1}, {"two", 2}};
    json j_object_empty(json::value_t::object);
    json j_array = {1, 2, 4, 8, 16};
    json j_array_empty(json::value_t::array);
    json j_string = "Hello, world";

    // call front()
    //std::cout << j_null.front() << '\n';          // would throw
    std::cout << j_boolean.front() << '\n';
    std::cout << j_number_integer.front() << '\n';
    std::cout << j_number_float.front() << '\n';
    std::cout << j_object.front() << '\n';
    //std::cout << j_object_empty.front() << '\n';  // undefined behavior
    std::cout << j_array.front() << '\n';
    //std::cout << j_array_empty.front() << '\n';   // undefined behavior
    std::cout << j_string.front() << '\n';
}
