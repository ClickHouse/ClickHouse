#pragma once

#include <common/types.h>
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
    IDocumentation(const String& doc_name, const String& doc_group);

    String getDocumentation() const;
    const std::vector<String>& getOrder() const;
    const std::vector<String>& getExamples() const;
    const std::vector<String>& getSettings() const;
    const std::vector<String>& getReferences() const;
    const std::vector<String>& getDescriptions() const;
    virtual ~IDocumentation() = default;


private:
    void addExample(const String& example);
    void addSetting(const String& setting);
    void addDescription(const String& description);
    void addReference(const String& reference);

    virtual String createDocumentation() const = 0;
    virtual String addHeader(const String& header_name) const = 0;

    String name;
    String group;

    std::vector<String> order;
    std::vector<String> settings;
    std::vector<String> examples;
    std::vector<String> descriptions;
    std::vector<String> references;
};

using IDocumentationPtr = std::shared_ptr<IDocumentation>;

}
