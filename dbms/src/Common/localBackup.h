#pragma once

#include <Poco/Path.h>
#include <optional>


/** Creates a local (at the same mount point) backup (snapshot) directory.
  *
  * In the specified destination directory, it creates a hard links on all source-directory files
  *  and in all nested directories, with saving (creating) all relative paths;
  *  and also `chown`, removing the write permission.
  *
  * This protects data from accidental deletion or modification,
  *  and is intended to be used as a simple means of protection against a human or program error,
  *  but not from a hardware failure.
  *
  *  If max_level is specified, than only files which depth relative source_path less or equal max_level will be copied.
  *  So, if max_level=0 than only direct file child are copied.
  */
void localBackup(const Poco::Path & source_path, const Poco::Path & destination_path, std::optional<size_t> max_level = {});

