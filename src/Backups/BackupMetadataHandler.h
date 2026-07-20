#pragma once

#include <base/types.h>

#include <Poco/SAX/DefaultHandler.h>
#include <Poco/SAX/Attributes.h>

#include <exception>
#include <functional>
#include <map>
#include <vector>


namespace DB
{

/// Streaming SAX handler that reads the `.backup` metadata document without materializing a DOM tree.
///
/// The document has a fixed, shallow shape:
///     <config>
///         <version/><timestamp/><uuid/> ... other header elements ...
///         <contents>
///             <file><name/><size/> ... </file>
///             ...
///         </contents>
///     </config>
///
/// All header elements precede `<contents>`, so `on_header` is fired once `<contents>` opens - before any
/// `<file>` is seen. Each `<file>` fires `on_file` as soon as it closes, so files are processed one at a time
/// instead of being retained in a DOM tree. Callbacks may throw; the exception is captured into `saved_exception`
/// and must be rethrown by the caller after parsing finishes, because exceptions must not propagate through the
/// underlying expat callbacks.
class BackupMetadataHandler : public Poco::XML::DefaultHandler
{
public:
    using Fields = std::map<String, String>;

    /// Fired once, when `<contents>` starts, with all top-level header elements collected.
    std::function<void(const Fields &)> on_header;
    /// Fired once per `<file>`, when it closes, with the file's leaf elements.
    std::function<void(const Fields &)> on_file;

    /// The first exception thrown by a callback, if any. The caller must rethrow it after parsing.
    std::exception_ptr saved_exception;

    void startElement(
        const Poco::XML::XMLString & uri,
        const Poco::XML::XMLString & local_name,
        const Poco::XML::XMLString & qname,
        const Poco::XML::Attributes & attributes) override;

    void endElement(
        const Poco::XML::XMLString & uri,
        const Poco::XML::XMLString & local_name,
        const Poco::XML::XMLString & qname) override;

    void characters(const Poco::XML::XMLChar ch[], int start, int length) override;

private:
    std::vector<String> path;
    String current_text;
    Fields header_fields;
    Fields file_fields;
    bool root_contents_seen = false;
};

}
