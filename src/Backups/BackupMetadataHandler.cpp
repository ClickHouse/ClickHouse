#include <Backups/BackupMetadataHandler.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BACKUP_DAMAGED;
}

void BackupMetadataHandler::startElement(
    const Poco::XML::XMLString &,
    const Poco::XML::XMLString & local_name,
    const Poco::XML::XMLString & qname,
    const Poco::XML::Attributes &)
{
    if (saved_exception)
        return;
    try
    {
        /// A scalar leaf (header leaf under the root, file leaf under <contents>/<file>) must be text-only:
        /// reject mixed content, which the SAX path would otherwise collapse to the last text run (turning a
        /// damaged value into a valid one).
        if ((path.size() == 2 && path[1] != "contents")
            || (path.size() == 4 && path[1] == "contents" && path[2] == "file"))
            throw Exception(
                ErrorCodes::BACKUP_DAMAGED, "Backup metadata has a child element inside scalar field <{}>", path.back());

        current_text.clear();
        const String & name = qname.empty() ? local_name : qname;
        /// Gate callbacks by exact position so a misplaced <file>/<contents> is ignored. `path` holds the
        /// ancestors; <contents> directly under the root fires on_header (all header leaves collected by then).
        if (name == "contents" && path.size() == 1)
        {
            /// A well-formed manifest has exactly one top-level <contents>. Reject a second one instead of
            /// re-applying the header and appending another file list (writeBackupMetadata never emits two).
            if (root_contents_seen)
                throw Exception(ErrorCodes::BACKUP_DAMAGED, "Backup metadata has more than one top-level <contents>");
            root_contents_seen = true;
            if (on_header)
                on_header(header_fields);
        }
        else if (name == "file" && path.size() == 2 && path[1] == "contents")
            file_fields.clear();
        path.push_back(name);
    }
    catch (...)
    {
        saved_exception = std::current_exception();
    }
}

void BackupMetadataHandler::endElement(
    const Poco::XML::XMLString &,
    const Poco::XML::XMLString & local_name,
    const Poco::XML::XMLString & qname)
{
    if (saved_exception)
        return;
    try
    {
        const String & name = qname.empty() ? local_name : qname;
        /// On a closing tag `path.back() == name`. Gate by exact position (see startElement).
        if (name == "file" && path.size() == 3 && path[1] == "contents")
        {
            if (on_file)
                on_file(file_fields);
        }
        else if (path.size() == 2 && name != "contents")  /// header leaf: <root>/<leaf>
            header_fields.try_emplace(name, current_text);  /// keep the first value, like the old DOM getNodeByPath
        else if (path.size() == 4 && path[1] == "contents" && path[2] == "file")  /// file leaf
            file_fields.try_emplace(name, current_text);
        current_text.clear();
        if (!path.empty())
            path.pop_back();
    }
    catch (...)
    {
        saved_exception = std::current_exception();
    }
}

void BackupMetadataHandler::characters(const Poco::XML::XMLChar ch[], int start, int length)
{
    if (saved_exception)
        return;
    try
    {
        current_text.append(ch + start, static_cast<size_t>(length));
    }
    catch (...)
    {
        saved_exception = std::current_exception();
    }
}

}
