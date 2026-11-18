#include <Storages/MergeTree/Manifest/BaseManifestEntry.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

void BaseManifestEntry::writeText(WriteBuffer & out) const
{
    out << "name: " << escape << name << '\n'
        << "state: " << state << '\n'
        << "disk_name: " << escape << disk_name << '\n'
        << "disk_path: " << escape << disk_path << '\n'
        << "part_uuid: " << escape << part_uuid << '\n';
}

void BaseManifestEntry::readText(ReadBuffer & in)
{
    in >> "name: " >> escape >> name >> "\n"
       >> "state: " >> state >> "\n"
       >> "disk_name: " >> escape >> disk_name >> "\n"
       >> "disk_path: " >> escape >> disk_path >> "\n"
       >> "part_uuid: " >> escape >> part_uuid >> "\n";
}

String BaseManifestEntry::toString() const
{
    WriteBufferFromOwnString out;
    writeText(out);
    return out.str();
}

}
