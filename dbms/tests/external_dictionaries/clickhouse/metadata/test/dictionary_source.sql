ATTACH TABLE dictionary_source
(
    id UInt64, 
    key0 UInt8, 
    key0_str String, 
    key1 UInt8, 
    UInt8_ UInt8, 
    UInt16_ UInt16, 
    UInt32_ UInt32, 
    UInt64_ UInt64, 
    Int8_ Int8, 
    Int16_ Int16, 
    Int32_ Int32, 
    Int64_ Int64, 
    Float32_ Float32, 
    Float64_ Float64, 
    String_ String, 
    Date_ Date, 
    DateTime_ DateTime, 
    Parent UInt64
) ENGINE = Log
