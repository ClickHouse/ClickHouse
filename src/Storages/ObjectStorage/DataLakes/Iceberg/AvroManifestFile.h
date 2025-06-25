#pragma once

#include <Decoder.hh>
#include <Encoder.hh>
#include <Specific.hh>

#include <any>

namespace Apache::Iceberg
{
struct SnapshotID
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    int64_t get_long() const;
    void set_long(const int64_t & v);

    SnapshotID();
};

struct SequenceNumber
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    int64_t get_long() const;
    void set_long(const int64_t & v);

    SequenceNumber();
};

struct FileSequenceNumber
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    int64_t get_long() const;
    void set_long(const int64_t & v);
    FileSequenceNumber();
};

struct Partition
{
    Partition() = default;
};

struct ColumnSize
{
    int32_t key;
    int64_t value;
    ColumnSize() : key(int32_t()), value(int64_t()) { }
};

struct ColumnSizes
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    std::vector<ColumnSize> get_array() const;
    void set_array(const std::vector<ColumnSize> & v);
    ColumnSizes();
};

struct ValueCount
{
    int32_t key;
    int64_t value;
    ValueCount() : key(int32_t()), value(int64_t()) { }
};

struct ValueCounts
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    std::vector<ValueCount> get_array() const;
    void set_array(const std::vector<ValueCount> & v);
    ValueCounts();
};

struct NullValueCount
{
    int32_t key;
    int64_t value;
    NullValueCount() : key(int32_t()), value(int64_t()) { }
};

struct NullValueCounts
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    std::vector<NullValueCount> get_array() const;
    void set_array(const std::vector<NullValueCount> & v);
    NullValueCounts();
};

struct NanValueCount
{
    int32_t key;
    int64_t value;
    NanValueCount() : key(int32_t()), value(int64_t()) { }
};

struct NanValueCounts
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    std::vector<NanValueCount> get_array() const;
    void set_array(const std::vector<NanValueCount> & v);
    NanValueCounts();
};

struct LowerBound
{
    int32_t key;
    std::vector<uint8_t> value;
    LowerBound() : key(int32_t()) { }
};

struct LowerBounds
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    std::vector<LowerBound> get_array() const;
    void set_array(const std::vector<LowerBound> & v);
    LowerBounds();
};

struct UpperBound
{
    int32_t key;
    std::vector<uint8_t> value;
    UpperBound() : key(int32_t()) { }
};

struct UpperBounds
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    std::vector<UpperBound> get_array() const;
    void set_array(const std::vector<UpperBound> & v);
    UpperBounds();
};

struct KeyMetadata
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    std::vector<uint8_t> get_bytes() const;
    void set_bytes(const std::vector<uint8_t> & v);
    KeyMetadata();
};

struct SplitOffsets
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    std::vector<int64_t> get_array() const;
    void set_array(const std::vector<int64_t> & v);
    SplitOffsets();
};

struct EqualityIds
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    std::vector<int32_t> get_array() const;
    void set_array(const std::vector<int32_t> & v);
    EqualityIds();
};

struct SortOrderId
{
private:
    size_t idx_;
    std::any value_;

public:
    size_t idx() const { return idx_; }
    bool is_null() const { return (idx_ == 0); }
    void set_null()
    {
        idx_ = 0;
        value_ = std::any();
    }
    int32_t get_int() const;
    void set_int(const int32_t & v);
    SortOrderId();
};

struct DataFile
{
    int32_t content;
    std::string file_path;
    std::string file_format;
    Partition partition;
    int64_t record_count;
    int64_t file_size_in_bytes;
    ColumnSizes column_sizes;
    ValueCounts value_counts;
    NullValueCounts null_value_counts;
    NanValueCounts nan_value_counts;
    LowerBounds lower_bounds;
    UpperBounds upper_bounds;
    KeyMetadata key_metadata;
    SplitOffsets split_offsets;
    EqualityIds equality_ids;
    SortOrderId sort_order_id;
    DataFile() : content(int32_t()), record_count(int64_t()), file_size_in_bytes(int64_t()) { }
};

struct Manifest
{
    int32_t status;
    SnapshotID snapshot_id;
    SequenceNumber sequence_number;
    FileSequenceNumber file_sequence_number;
    DataFile data_file;
    Manifest() : status(int32_t()) { }
};

inline int64_t SnapshotID::get_long() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__0__");
    }
    return std::any_cast<int64_t>(value_);
}

inline void SnapshotID::set_long(const int64_t & v)
{
    idx_ = 1;
    value_ = v;
}

inline int64_t SequenceNumber::get_long() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__1__");
    }
    return std::any_cast<int64_t>(value_);
}

inline void SequenceNumber::set_long(const int64_t & v)
{
    idx_ = 1;
    value_ = v;
}

inline int64_t FileSequenceNumber::get_long() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__2__");
    }
    return std::any_cast<int64_t>(value_);
}

inline void FileSequenceNumber::set_long(const int64_t & v)
{
    idx_ = 1;
    value_ = v;
}

inline std::vector<ColumnSize> ColumnSizes::get_array() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__3__");
    }
    return std::any_cast<std::vector<ColumnSize>>(value_);
}

inline void ColumnSizes::set_array(const std::vector<ColumnSize> & v)
{
    idx_ = 1;
    value_ = v;
}

inline std::vector<ValueCount> ValueCounts::get_array() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__4__");
    }
    return std::any_cast<std::vector<ValueCount>>(value_);
}

inline void ValueCounts::set_array(const std::vector<ValueCount> & v)
{
    idx_ = 1;
    value_ = v;
}

inline std::vector<NullValueCount> NullValueCounts::get_array() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__5__");
    }
    return std::any_cast<std::vector<NullValueCount>>(value_);
}

inline void NullValueCounts::set_array(const std::vector<NullValueCount> & v)
{
    idx_ = 1;
    value_ = v;
}

inline std::vector<NanValueCount> NanValueCounts::get_array() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__6__");
    }
    return std::any_cast<std::vector<NanValueCount>>(value_);
}

inline void NanValueCounts::set_array(const std::vector<NanValueCount> & v)
{
    idx_ = 1;
    value_ = v;
}

inline std::vector<LowerBound> LowerBounds::get_array() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__7__");
    }
    return std::any_cast<std::vector<LowerBound>>(value_);
}

inline void LowerBounds::set_array(const std::vector<LowerBound> & v)
{
    idx_ = 1;
    value_ = v;
}

inline std::vector<UpperBound> UpperBounds::get_array() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__8__");
    }
    return std::any_cast<std::vector<UpperBound>>(value_);
}

inline void UpperBounds::set_array(const std::vector<UpperBound> & v)
{
    idx_ = 1;
    value_ = v;
}

inline std::vector<uint8_t> KeyMetadata::get_bytes() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__9__");
    }
    return std::any_cast<std::vector<uint8_t>>(value_);
}

inline void KeyMetadata::set_bytes(const std::vector<uint8_t> & v)
{
    idx_ = 1;
    value_ = v;
}

inline std::vector<int64_t> SplitOffsets::get_array() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__10__");
    }
    return std::any_cast<std::vector<int64_t>>(value_);
}

inline void SplitOffsets::set_array(const std::vector<int64_t> & v)
{
    idx_ = 1;
    value_ = v;
}

inline std::vector<int32_t> EqualityIds::get_array() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__11__");
    }
    return std::any_cast<std::vector<int32_t>>(value_);
}

inline void EqualityIds::set_array(const std::vector<int32_t> & v)
{
    idx_ = 1;
    value_ = v;
}

inline int32_t SortOrderId::get_int() const
{
    if (idx_ != 1)
    {
        throw avro::Exception("Invalid type for union cpx_json_Union__12__");
    }
    return std::any_cast<int32_t>(value_);
}

inline void SortOrderId::set_int(const int32_t & v)
{
    idx_ = 1;
    value_ = v;
}

inline SnapshotID::SnapshotID() : idx_(0)
{
}
inline SequenceNumber::SequenceNumber() : idx_(0)
{
}
inline FileSequenceNumber::FileSequenceNumber() : idx_(0)
{
}
inline ColumnSizes::ColumnSizes() : idx_(0)
{
}
inline ValueCounts::ValueCounts() : idx_(0)
{
}
inline NullValueCounts::NullValueCounts() : idx_(0)
{
}
inline NanValueCounts::NanValueCounts() : idx_(0)
{
}
inline LowerBounds::LowerBounds() : idx_(0)
{
}
inline UpperBounds::UpperBounds() : idx_(0)
{
}
inline KeyMetadata::KeyMetadata() : idx_(0)
{
}
inline SplitOffsets::SplitOffsets() : idx_(0)
{
}
inline EqualityIds::EqualityIds() : idx_(0)
{
}
inline SortOrderId::SortOrderId() : idx_(0)
{
}
}
namespace avro
{
template <>
struct codec_traits<Apache::Iceberg::SnapshotID>
{
    static void encode(Encoder & e, Apache::Iceberg::SnapshotID v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_long());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::SnapshotID & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                int64_t vv;
                avro::decode(d, vv);
                v.set_long(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::SequenceNumber>
{
    static void encode(Encoder & e, Apache::Iceberg::SequenceNumber v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_long());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::SequenceNumber & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                int64_t vv;
                avro::decode(d, vv);
                v.set_long(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::FileSequenceNumber>
{
    static void encode(Encoder & e, Apache::Iceberg::FileSequenceNumber v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_long());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::FileSequenceNumber & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                int64_t vv;
                avro::decode(d, vv);
                v.set_long(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::Partition>
{
    static void encode(Encoder & /*e*/, const Apache::Iceberg::Partition & /*v*/) { }
    static void decode(Decoder & d, Apache::Iceberg::Partition & /*v*/)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                switch (it)
                {
                    default:
                        break;
                }
            }
        }
        else
        {
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::ColumnSize>
{
    static void encode(Encoder & e, const Apache::Iceberg::ColumnSize & v)
    {
        avro::encode(e, v.key);
        avro::encode(e, v.value);
    }
    static void decode(Decoder & d, Apache::Iceberg::ColumnSize & v)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                switch (it)
                {
                    case 0:
                        avro::decode(d, v.key);
                        break;
                    case 1:
                        avro::decode(d, v.value);
                        break;
                    default:
                        break;
                }
            }
        }
        else
        {
            avro::decode(d, v.key);
            avro::decode(d, v.value);
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::ColumnSizes>
{
    static void encode(Encoder & e, Apache::Iceberg::ColumnSizes v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_array());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::ColumnSizes & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                std::vector<Apache::Iceberg::ColumnSize> vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::ValueCount>
{
    static void encode(Encoder & e, const Apache::Iceberg::ValueCount & v)
    {
        avro::encode(e, v.key);
        avro::encode(e, v.value);
    }
    static void decode(Decoder & d, Apache::Iceberg::ValueCount & v)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                switch (it)
                {
                    case 0:
                        avro::decode(d, v.key);
                        break;
                    case 1:
                        avro::decode(d, v.value);
                        break;
                    default:
                        break;
                }
            }
        }
        else
        {
            avro::decode(d, v.key);
            avro::decode(d, v.value);
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::ValueCounts>
{
    static void encode(Encoder & e, Apache::Iceberg::ValueCounts v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_array());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::ValueCounts & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                std::vector<Apache::Iceberg::ValueCount> vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::NullValueCount>
{
    static void encode(Encoder & e, const Apache::Iceberg::NullValueCount & v)
    {
        avro::encode(e, v.key);
        avro::encode(e, v.value);
    }
    static void decode(Decoder & d, Apache::Iceberg::NullValueCount & v)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                switch (it)
                {
                    case 0:
                        avro::decode(d, v.key);
                        break;
                    case 1:
                        avro::decode(d, v.value);
                        break;
                    default:
                        break;
                }
            }
        }
        else
        {
            avro::decode(d, v.key);
            avro::decode(d, v.value);
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::NullValueCounts>
{
    static void encode(Encoder & e, Apache::Iceberg::NullValueCounts v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_array());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::NullValueCounts & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                std::vector<Apache::Iceberg::NullValueCount> vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::NanValueCount>
{
    static void encode(Encoder & e, const Apache::Iceberg::NanValueCount & v)
    {
        avro::encode(e, v.key);
        avro::encode(e, v.value);
    }
    static void decode(Decoder & d, Apache::Iceberg::NanValueCount & v)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                switch (it)
                {
                    case 0:
                        avro::decode(d, v.key);
                        break;
                    case 1:
                        avro::decode(d, v.value);
                        break;
                    default:
                        break;
                }
            }
        }
        else
        {
            avro::decode(d, v.key);
            avro::decode(d, v.value);
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::NanValueCounts>
{
    static void encode(Encoder & e, Apache::Iceberg::NanValueCounts v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_array());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::NanValueCounts & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                std::vector<Apache::Iceberg::NanValueCount> vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::LowerBound>
{
    static void encode(Encoder & e, const Apache::Iceberg::LowerBound & v)
    {
        avro::encode(e, v.key);
        avro::encode(e, v.value);
    }
    static void decode(Decoder & d, Apache::Iceberg::LowerBound & v)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                switch (it)
                {
                    case 0:
                        avro::decode(d, v.key);
                        break;
                    case 1:
                        avro::decode(d, v.value);
                        break;
                    default:
                        break;
                }
            }
        }
        else
        {
            avro::decode(d, v.key);
            avro::decode(d, v.value);
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::LowerBounds>
{
    static void encode(Encoder & e, Apache::Iceberg::LowerBounds v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_array());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::LowerBounds & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                std::vector<Apache::Iceberg::LowerBound> vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::UpperBound>
{
    static void encode(Encoder & e, const Apache::Iceberg::UpperBound & v)
    {
        avro::encode(e, v.key);
        avro::encode(e, v.value);
    }
    static void decode(Decoder & d, Apache::Iceberg::UpperBound & v)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                switch (it)
                {
                    case 0:
                        avro::decode(d, v.key);
                        break;
                    case 1:
                        avro::decode(d, v.value);
                        break;
                    default:
                        break;
                }
            }
        }
        else
        {
            avro::decode(d, v.key);
            avro::decode(d, v.value);
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::UpperBounds>
{
    static void encode(Encoder & e, Apache::Iceberg::UpperBounds v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_array());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::UpperBounds & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                std::vector<Apache::Iceberg::UpperBound> vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::KeyMetadata>
{
    static void encode(Encoder & e, Apache::Iceberg::KeyMetadata v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_bytes());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::KeyMetadata & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                std::vector<uint8_t> vv;
                avro::decode(d, vv);
                v.set_bytes(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::SplitOffsets>
{
    static void encode(Encoder & e, Apache::Iceberg::SplitOffsets v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_array());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::SplitOffsets & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                std::vector<int64_t> vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::EqualityIds>
{
    static void encode(Encoder & e, Apache::Iceberg::EqualityIds v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_array());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::EqualityIds & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                std::vector<int32_t> vv;
                avro::decode(d, vv);
                v.set_array(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::SortOrderId>
{
    static void encode(Encoder & e, Apache::Iceberg::SortOrderId v)
    {
        e.encodeUnionIndex(v.idx());
        switch (v.idx())
        {
            case 0:
                e.encodeNull();
                break;
            case 1:
                avro::encode(e, v.get_int());
                break;
        }
    }
    static void decode(Decoder & d, Apache::Iceberg::SortOrderId & v)
    {
        size_t n = d.decodeUnionIndex();
        if (n >= 2)
        {
            throw avro::Exception("Union index too big");
        }
        switch (n)
        {
            case 0:
                d.decodeNull();
                v.set_null();
                break;
            case 1:
            {
                int32_t vv;
                avro::decode(d, vv);
                v.set_int(vv);
            }
            break;
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::DataFile>
{
    static void encode(Encoder & e, const Apache::Iceberg::DataFile & v)
    {
        avro::encode(e, v.content);
        avro::encode(e, v.file_path);
        avro::encode(e, v.file_format);
        avro::encode(e, v.partition);
        avro::encode(e, v.record_count);
        avro::encode(e, v.file_size_in_bytes);
        avro::encode(e, v.column_sizes);
        avro::encode(e, v.value_counts);
        avro::encode(e, v.null_value_counts);
        avro::encode(e, v.nan_value_counts);
        avro::encode(e, v.lower_bounds);
        avro::encode(e, v.upper_bounds);
        avro::encode(e, v.key_metadata);
        avro::encode(e, v.split_offsets);
        avro::encode(e, v.equality_ids);
        avro::encode(e, v.sort_order_id);
    }
    static void decode(Decoder & d, Apache::Iceberg::DataFile & v)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                switch (it)
                {
                    case 0:
                        avro::decode(d, v.content);
                        break;
                    case 1:
                        avro::decode(d, v.file_path);
                        break;
                    case 2:
                        avro::decode(d, v.file_format);
                        break;
                    case 3:
                        avro::decode(d, v.partition);
                        break;
                    case 4:
                        avro::decode(d, v.record_count);
                        break;
                    case 5:
                        avro::decode(d, v.file_size_in_bytes);
                        break;
                    case 6:
                        avro::decode(d, v.column_sizes);
                        break;
                    case 7:
                        avro::decode(d, v.value_counts);
                        break;
                    case 8:
                        avro::decode(d, v.null_value_counts);
                        break;
                    case 9:
                        avro::decode(d, v.nan_value_counts);
                        break;
                    case 10:
                        avro::decode(d, v.lower_bounds);
                        break;
                    case 11:
                        avro::decode(d, v.upper_bounds);
                        break;
                    case 12:
                        avro::decode(d, v.key_metadata);
                        break;
                    case 13:
                        avro::decode(d, v.split_offsets);
                        break;
                    case 14:
                        avro::decode(d, v.equality_ids);
                        break;
                    case 15:
                        avro::decode(d, v.sort_order_id);
                        break;
                    default:
                        break;
                }
            }
        }
        else
        {
            avro::decode(d, v.content);
            avro::decode(d, v.file_path);
            avro::decode(d, v.file_format);
            avro::decode(d, v.partition);
            avro::decode(d, v.record_count);
            avro::decode(d, v.file_size_in_bytes);
            avro::decode(d, v.column_sizes);
            avro::decode(d, v.value_counts);
            avro::decode(d, v.null_value_counts);
            avro::decode(d, v.nan_value_counts);
            avro::decode(d, v.lower_bounds);
            avro::decode(d, v.upper_bounds);
            avro::decode(d, v.key_metadata);
            avro::decode(d, v.split_offsets);
            avro::decode(d, v.equality_ids);
            avro::decode(d, v.sort_order_id);
        }
    }
};

template <>
struct codec_traits<Apache::Iceberg::Manifest>
{
    static void encode(Encoder & e, const Apache::Iceberg::Manifest & v)
    {
        avro::encode(e, v.status);
        avro::encode(e, v.snapshot_id);
        avro::encode(e, v.sequence_number);
        avro::encode(e, v.file_sequence_number);
        avro::encode(e, v.data_file);
    }
    static void decode(Decoder & d, Apache::Iceberg::Manifest & v)
    {
        if (avro::ResolvingDecoder * rd = dynamic_cast<avro::ResolvingDecoder *>(&d))
        {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (auto it : fo)
            {
                switch (it)
                {
                    case 0:
                        avro::decode(d, v.status);
                        break;
                    case 1:
                        avro::decode(d, v.snapshot_id);
                        break;
                    case 2:
                        avro::decode(d, v.sequence_number);
                        break;
                    case 3:
                        avro::decode(d, v.file_sequence_number);
                        break;
                    case 4:
                        avro::decode(d, v.data_file);
                        break;
                    default:
                        break;
                }
            }
        }
        else
        {
            avro::decode(d, v.status);
            avro::decode(d, v.snapshot_id);
            avro::decode(d, v.sequence_number);
            avro::decode(d, v.file_sequence_number);
            avro::decode(d, v.data_file);
        }
    }
};

}
