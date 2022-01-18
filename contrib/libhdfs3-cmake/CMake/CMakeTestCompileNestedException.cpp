#include <exception>
#include <stdexcept>

int main() {
    try {
        throw 2;
    } catch (int) {
        std::throw_with_nested(std::runtime_error("test"));
    }
}
