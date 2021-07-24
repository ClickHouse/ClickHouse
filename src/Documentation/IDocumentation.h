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
    virtual ~IDocumentation() = default;

    ///add examples in documentation and remember example
    void addExample(const String& input, const String& query, const String& result);
    
    /// Simple adding description part
    void addDescription(const String& header, const String& description);
    
    /// Add reference in correct style
    void addReference(const String& ref_name, const String& source);
    
    /// Use this function to add sources to the end of documentation
    void addReferencesToDocs();

    /// Add header in correct style (depends on output type)
    void addHeader(const String& header_name);

    static String createCodeSection(const String& code);

private:
    /// Create header in correct style (depends on output type)
    virtual String createHeader(const String& header_name) const = 0;

    /// Create correct type of reference (for example markdown)
    virtual String createReference(const String& ref_name, const String& source) const = 0;

    String name;
    String group;
    String documentation;

    /// Example consists of this three fields.
    /// Separate them from other documentation make test generation easier
    std::vector<String> inputs;
    std::vector<String> queries;
    std::vector<String> results;
    
    std::vector<String> references;
    std::vector<String> sources;
};

using IDocumentationPtr = std::shared_ptr<IDocumentation>;

}
