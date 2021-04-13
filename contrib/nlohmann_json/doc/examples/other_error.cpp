#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

int main()
{
    try
    {
        // executing a failing JSON Patch operation
        json value = R"({
            "best_biscuit": {
                "name": "Oreo"
            }
        })"_json;
        json patch = R"([{
            "op": "test",
            "path": "/best_biscuit/name",
            "value": "Choco Leibniz"
        }])"_json;
        value.patch(patch);
    }
    catch (json::other_error& e)
    {
        // output exception information
        std::cout << "message: " << e.what() << '\n'
                  << "exception id: " << e.id << std::endl;
    }
}
