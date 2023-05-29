#include <archive.h>
#include <archive_entry.h>

bool create_tar_with_file(const std::string &archivename, std::map<std::string, std::string> files) {
    struct archive *a;
    struct archive_entry *entry;

    a = archive_write_new();
    archive_write_set_format_pax_restricted(a);
    archive_write_open_filename(a, archivename.c_str());

    for (auto &[filename, content] : files) {
        entry = archive_entry_new();
        archive_entry_set_pathname(entry, filename.c_str());
        archive_entry_set_size(entry, content.size());
        archive_entry_set_mode(entry, S_IFREG | 0644); // regular file with rw-r--r-- permissions
        archive_entry_set_mtime(entry, time(nullptr), 0);
        archive_write_header(a, entry);
        archive_write_data(a, content.c_str(), content.size());
        archive_entry_free(entry);
    }
    
    archive_write_close(a);
    archive_write_free(a);

    return true;
}
