#include "IonReader.h"

#if USE_ION

#include <decNumber/decContext.h>
#include <Common/formatIPv6.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int ILLEGAL_COLUMN;
}

struct IonDecimalDeleter
{
    void operator()(ION_DECIMAL * d)
    {
        ion_decimal_free(d);
        free(d);
    }
};

struct IonIntDeleter
{
    void operator()(ION_INT * i)
    {
        ion_int_free(i);
        free(i);
    }
};

struct IonTimestampDeleter
{
    void operator()(ION_TIMESTAMP * t) { free(t); }
};

IonReader::IonReader(ReadBuffer & in_) : in(in_)
{
    ION_READER_OPTIONS options;
    memset(&options, 0, sizeof(options));
    this->current_decimal_context = {
        MAX_DECIMAL_DIGITS, /* max digits */
        DEC_MAX_MATH, /* max exponent */
        -DEC_MAX_MATH, /* min exponent */
        DEC_ROUND_HALF_EVEN, /* rounding mode */
        DEC_Errors, /* trap conditions */
        0, /* status flags */
        0 /* apply exponent clamp? */
    };
    options.decimal_context = &(this->current_decimal_context);
    hREADER _reader = nullptr;

    auto err = ion_reader_open_buffer(
        &_reader, reinterpret_cast<unsigned char *>(in_.buffer().begin()), static_cast<SIZE>(in_.buffer().size()), &options);
    if (err == IERR_OK)
        reader = _reader;
    else
        printf("ERROR\n");
}

IonReader::~IonReader()
{
    if (reader)
        ion_reader_close(reader);
}

ION_TYPE IonReader::currentType() const
{
    return current_type;
}

std::optional<std::string> IonReader::fieldName() const
{
    ION_STRING ion_str;
    iERR err = ion_reader_get_field_name(reader, &ion_str);
    if (err == IERR_OK)
    {
        std::string value(reinterpret_cast<char *>(ion_str.value), ion_str.length);
        return std::make_optional(value);
    }
    return std::nullopt;
};

iERR IonReader::next()
{
    auto err = ion_reader_next(reader, &current_type);
    POSITION current_value_offset;
    ion_reader_get_value_offset(reader, &current_value_offset);
    in.position() = in.buffer().begin() + current_value_offset;
    return err;
}

bool IonReader::inStruct()
{
    BOOL in_struct = FALSE;
    auto err = ion_reader_is_in_struct(reader, &in_struct);
    if (err != IERR_OK)
        printf("ERROR calling is_in_struct: %d\n", err);
    return in_struct == TRUE;
}

iERR IonReader::stepIn()
{
    return ion_reader_step_in(reader);
}

iERR IonReader::stepOut()
{
    return ion_reader_step_out(reader);
}

int IonReader::depth() const
{
    int depth = 0;
    ion_reader_get_depth(reader, &depth);
    return depth;
}

bool IonReader::isNull() const
{
    BOOL is_null = FALSE;
    auto err = ion_reader_is_null(reader, &is_null);
    if (err != IERR_OK)
        printf("ERROR is_null: %d", err);
    return (is_null == TRUE);
}

std::vector<std::string> IonReader::getAnnotations()
{
    std::vector<std::string> annot;
    BOOL has_anns = FALSE;
    SIZE count = 0;

    ion_reader_has_any_annotations(reader, &has_anns);
    if (has_anns == TRUE)
    {
        ion_reader_get_annotation_count(reader, &count);
        ION_STRING * syms = new ION_STRING[count];
        ion_reader_get_annotations(reader, syms, count, &count);
        for (int i = 0; i < count; i++)
            annot.push_back(std::string(reinterpret_cast<char *>(syms[i].value), syms[i].length));
        delete[] syms;
    }
    return annot;
}

iERR IonReader::read(std::string & val)
{
    ION_STRING ion_str;
    auto err = ion_reader_read_string(reader, &ion_str);
    val.assign(reinterpret_cast<char *>(ion_str.value), ion_str.length);
    return err;
}

iERR IonReader::read(int32_t & val)
{
    return ion_reader_read_int(reader, &val);
}

iERR IonReader::read(int64_t & val)
{
    return ion_reader_read_int64(reader, &val);
}

iERR IonReader::read(IonNull & val)
{
    ION_TYPE tpe;
    auto err = ion_reader_read_null(reader, &tpe);
    val.tpe = tpe;
    return err;
}

static IonDecimalPtr makeIonDecimal()
{
    ION_DECIMAL * dec = reinterpret_cast<ION_DECIMAL *>(malloc(sizeof(ION_DECIMAL)));
    IonDecimalPtr val;
    val.reset(dec, IonDecimalDeleter());
    return val;
}

static IonIntPtr makeIonInt()
{
    ION_INT original;
    ion_int_init(&original, nullptr);
    ION_INT * integer = reinterpret_cast<ION_INT *>(malloc(sizeof(ION_INT)));
    auto err = ion_int_copy(integer, &original, nullptr);
    if (err != IERR_OK)
        free(integer);
    IonIntPtr val;
    val.reset(integer, IonIntDeleter());
    return val;
}

iERR IonReader::read(IonDecimalPtr & val)
{
    ION_DECIMAL original;
    auto err = ion_reader_read_ion_decimal(reader, &original);
    if (err != IERR_OK)
        return err;

    ION_DECIMAL * dec = reinterpret_cast<ION_DECIMAL *>(malloc(sizeof(ION_DECIMAL)));
    err = ion_decimal_copy(dec, &original);
    if (err != IERR_OK)
    {
        free(dec);
        return err;
    }
    val.reset(dec, IonDecimalDeleter());
    return err;
}

iERR IonReader::read(IonIntPtr & val)
{
    ION_INT original;
    ion_int_init(&original, reader);
    auto err = ion_reader_read_ion_int(reader, &original);
    if (err != IERR_OK)
        return err;

    ION_INT * integer = reinterpret_cast<ION_INT *>(malloc(sizeof(ION_INT)));
    err = ion_int_copy(integer, &original, reader);
    if (err != IERR_OK)
    {
        free(integer);
        return err;
    }
    val.reset(integer, IonIntDeleter());
    return err;
}

IonIntPtr IonReader::convertDecimalToInt(IonDecimalPtr & val)
{
    auto integral_value_dec = makeIonDecimal();
    ion_decimal_to_integral_value(val.get(), integral_value_dec.get(), &current_decimal_context);
    auto ion_int = makeIonInt();
    ion_decimal_to_ion_int(integral_value_dec.get(), &current_decimal_context, ion_int.get());
    return ion_int;
}

iERR IonReader::read(IonTimestampPtr & val)
{
    ION_TIMESTAMP * ts = reinterpret_cast<ION_TIMESTAMP *>(malloc(sizeof(ION_TIMESTAMP)));
    auto err = ion_reader_read_timestamp(reader, ts);
    if (err != IERR_OK)
        return err;
    val.reset(ts, IonTimestampDeleter());
    return IERR_OK;
}


iERR IonReader::read(double & val)
{
    return ion_reader_read_double(reader, &val);
}

iERR IonReader::read(bool & val)
{
    BOOL b = FALSE;
    auto err = ion_reader_read_bool(reader, &b);
    if (err != IERR_OK)
        return err;
    val = (b == TRUE);
    return IERR_OK;
}

iERR IonReader::read(Symbol & val)
{
    ION_SYMBOL sym;
    auto err = ion_reader_read_ion_symbol(reader, &sym);
    if (err != IERR_OK)
        return err;

    ION_STRING ion_str;
    err = ion_reader_read_string(reader, &ion_str);
    if (err != IERR_OK)
        return err;

    std::string value(reinterpret_cast<char *>(ion_str.value), ion_str.length);
    val.value.assign(reinterpret_cast<char *>(ion_str.value), ion_str.length);

    return IERR_OK;
}

}

#endif
