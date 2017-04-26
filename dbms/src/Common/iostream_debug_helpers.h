#pragma once
#include <iostream>

namespace DB { class IBlockInputStream; }
std::ostream & operator<<(std::ostream & stream, const DB::IBlockInputStream & what);

namespace DB { struct NameAndTypePair; }
std::ostream & operator<<(std::ostream & stream, const DB::NameAndTypePair & what);

namespace DB { class Field; }
std::ostream & operator<<(std::ostream & stream, const DB::Field & what);

namespace DB { class IDataType; }
std::ostream & operator<<(std::ostream & stream, const DB::IDataType & what);

namespace DB { class IStorage; }
std::ostream & operator<<(std::ostream & stream, const DB::IStorage & what);

namespace DB { class TableStructureReadLock; }
std::ostream & operator<<(std::ostream & stream, const DB::TableStructureReadLock & what);

/// some operator<< should be declared before operator<<(... std::shared_ptr<>)
#include <common/iostream_debug_helpers.h>
