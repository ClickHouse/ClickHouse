#include <memory>
#include <string>


namespace DB
{
class ICommand;

using CommandPtr = std::shared_ptr<ICommand>;
}
