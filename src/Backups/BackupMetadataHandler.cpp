#include <Backups/BackupMetadataHandler.h>


namespace DB
{

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
        current_text.clear();
        const String & name = qname.empty() ? local_name : qname;
        if (name == "contents")
        {
            if (on_header)
                on_header(header_fields);
        }
        else if (name == "file")
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
        const size_t depth = path.size();
        if (name == "file")
        {
            if (on_file)
                on_file(file_fields);
        }
        else if (name == "contents")
        {
            /// Nothing to collect; header was already applied when it opened.
        }
        else if (depth == 2)  /// header leaf: <config>/<name>
            header_fields[name] = current_text;
        else if (depth == 4)  /// file leaf: <config>/<contents>/<file>/<name>
            file_fields[name] = current_text;
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
