#pragma once

#include <common/types.h>
#include <memory>
#include <string>
#include <vector>

namespace DB
{

/**
 * Class for creating documentation in order that is set by add-functions
 * It is simpler to build documention without missing symbols when changing docs,
 * easier to change order and to get particular parts of documentation for other uses
 */
class IDocumentation : public std::enable_shared_from_this<IDocumentation>
{
public:
    explicit IDocumentation(const char * doc): documentation(doc) {}

    const char* getDocumentation() const { return documentation; }
private:
    const char* documentation;
};

using IDocumentationPtr = std::unique_ptr<IDocumentation>;

}
