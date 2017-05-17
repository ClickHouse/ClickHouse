#pragma once

#include <Poco/Path.h>


/** Creates a local (at the same mount point) backup (snapshot) directory.
  *
  * In the specified destination directory, it creates a hard links on all source-directory files
  *  and in all nested directories, with saving (creating) all relative paths;
  *  and also `chown`, removing the write permission.
  *
  * This protects data from accidental deletion or modification,
  *  and is intended to be used as a simple means of protection against a human or program error,
  *  but not from a hardware failure.
  */
void localBackup(Poco::Path source_path, Poco::Path destination_path);
