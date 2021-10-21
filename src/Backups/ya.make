# This file is generated automatically, do not edit. See 'ya.make.in' and use 'utils/generate-ya-make' to regenerate it.
OWNER(g:clickhouse)

LIBRARY()

PEERDIR(
    clickhouse/src/Common
)


SRCS(
    BackupEntryConcat.cpp
    BackupEntryFromAppendOnlyFile.cpp
    BackupEntryFromImmutableFile.cpp
    BackupEntryFromMemory.cpp
    BackupEntryFromSmallFile.cpp
    BackupFactory.cpp
    BackupInDirectory.cpp
    BackupRenamingConfig.cpp
    BackupSettings.cpp
    BackupUtils.cpp
    hasCompatibleDataToRestoreTable.cpp
    renameInCreateQuery.cpp

)

END()
