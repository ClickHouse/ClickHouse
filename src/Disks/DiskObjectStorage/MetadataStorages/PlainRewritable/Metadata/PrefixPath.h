#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>

#include <string>
#include <string_view>
#include <vector>

namespace DB
{

/** The contents of a `prefix.path` object, which maps the random remote prefix of a directory to its logical path.
  *
  * The object has two forms.
  *
  * In the implicit form it contains just the logical path of the directory:
  *
  *     /hello/world/
  *
  * The files of the directory are the blobs stored under the remote prefix of the directory, under their own names.
  *
  * In the explicit form the logical path is followed by the list of files with the keys of their blobs
  * (relative to the common key prefix of the disk) and their sizes:
  *
  *     /hello/world/
  *     files: 2
  *     hello.json    gfkoqxvyhaasroiodbeurnftnwieiihy/hello.json    1234
  *     upyachka.bin  aaealinyzgdzycgcnpgaapdssrjirnnr/upyachka.bin  567
  *
  * The file names and the blob keys are escaped as in the `TSV` format. The blobs may live under a different prefix
  * (a hard link to a file of another directory), and the blobs stored under the prefix but absent from the list
  * do not belong to the directory. The explicit form is used only for directories that have (or had) hard links,
  * so a disk without hard links keeps the implicit form everywhere and stays readable by older versions.
  */
struct PrefixPathContents
{
    struct File
    {
        std::string name;
        std::string blob_key;
        size_t bytes_size = 0;
    };

    std::string logical_path;
    bool has_explicit_file_list = false;
    std::vector<File> files;
};

std::string serializePrefixPath(const std::string & logical_path, const DirectoryRemoteInfo & directory);
PrefixPathContents parsePrefixPath(std::string_view contents);

}
