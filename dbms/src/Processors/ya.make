LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    Executors/PipelineExecutor.cpp
    Formats/IOutputFormat.cpp
    Formats/LazyOutputFormat.cpp
    ISink.cpp
    ISource.cpp
    Pipe.cpp
    Port.cpp
    QueryPipeline.cpp
    Sources/SinkToOutputStream.cpp
    Sources/SourceFromInputStream.cpp
)

END()
