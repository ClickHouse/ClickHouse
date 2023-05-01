#pragma once

namespace DB
{

namespace RemoteFSProtocol
{
    enum PacketType 
    {
        Hello = 0,
        Ping = 1,
        Pong = 2,
        GetTotalSpace = 3,
        GetAvailableSpace = 4,
        Exists = 5,
        IsFile = 6,
        IsDirectory = 7,
        GetFileSize = 8,
        CreateDirectory = 9,
        CreateDirectories = 10,
        ClearDirectory = 11,
        MoveDirectory = 12,
        StartIterateDirectory = 13,
        EndIterateDirectory = 113,
        CreateFile = 14,
        MoveFile = 15,
        ReplaceFile = 16,
        Copy = 17,
        CopyDirectoryContent = 18, // TODO: fix test
        ListFiles = 19,
        ReadFile = 20, // TODO: improve
        StartWriteFile = 21, // TODO: improve
        EndWriteFile = 121,
        RemoveFile = 22,
        RemoveFileIfExists = 23,
        RemoveDirectory = 24,
        RemoveRecursive = 25,
        SetLastModified = 26,
        GetLastModified = 27,
        GetLastChanged = 28,
        SetReadOnly = 29, // TODO fix test
        CreateHardLink = 30,
        TruncateFile = 31,
        DataPacket = 55,
        Exception = 255
    };
}

}
