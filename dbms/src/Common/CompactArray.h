#pragma once

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_AVAILABLE_DATA;
}


/** Компактный массив для хранения данных, размер content_width, в битах, которых составляет
  * меньше одного байта. Вместо того, чтобы хранить каждое значение в отдельный
  * байт, что приводит к растрате 37.5% пространства для content_width=5, CompactArray хранит
  * смежные content_width-битные значения в массиве байтов, т.е. фактически CompactArray
  * симулирует массив content_width-битных значений.
  */
template <typename BucketIndex, UInt8 content_width, size_t bucket_count>
class __attribute__ ((packed)) CompactArray final
{
public:
    class Reader;
    class Locus;

public:
    CompactArray() = default;

    UInt8 ALWAYS_INLINE operator[](BucketIndex bucket_index) const
    {
        Locus locus(bucket_index);

        if (locus.index_l == locus.index_r)
            return locus.read(bitset[locus.index_l]);
        else
            return locus.read(bitset[locus.index_l], bitset[locus.index_r]);
    }

    Locus ALWAYS_INLINE operator[](BucketIndex bucket_index)
    {
        Locus locus(bucket_index);

        locus.content_l = &bitset[locus.index_l];

        if (locus.index_l == locus.index_r)
            locus.content_r = locus.content_l;
        else
            locus.content_r = &bitset[locus.index_r];

        return locus;
    }

    void readText(ReadBuffer & in)
    {
        for (size_t i = 0; i < BITSET_SIZE; ++i)
        {
            if (i != 0)
                assertChar(',', in);
            readIntText(bitset[i], in);
        }
    }

    void writeText(WriteBuffer & out) const
    {
        for (size_t i = 0; i < BITSET_SIZE; ++i)
        {
            if (i != 0)
                writeCString(",", out);
            writeIntText(bitset[i], out);
        }
    }

private:
    /// число байт в битсете
    static constexpr size_t BITSET_SIZE = (static_cast<size_t>(bucket_count) * content_width + 7) / 8;
    UInt8 bitset[BITSET_SIZE] = { 0 };
};

/** Класс для последовательного чтения ячеек из компактного массива на диске.
  */
template <typename BucketIndex, UInt8 content_width, size_t bucket_count>
class CompactArray<BucketIndex, content_width, bucket_count>::Reader final
{
public:
    Reader(ReadBuffer & in_)
        : in(in_)
    {
    }

    Reader(const Reader &) = delete;
    Reader & operator=(const Reader &) = delete;

    bool next()
    {
        if (current_bucket_index == bucket_count)
        {
            is_eof = true;
            return false;
        }

        locus.init(current_bucket_index);

        if (current_bucket_index == 0)
        {
            in.readStrict(reinterpret_cast<char *>(&value_l), 1);
            ++read_count;
        }
        else
            value_l = value_r;

        if (locus.index_l != locus.index_r)
        {
            if (read_count == BITSET_SIZE)
                fits_in_byte = true;
            else
            {
                fits_in_byte = false;
                in.readStrict(reinterpret_cast<char *>(&value_r), 1);
                ++read_count;
            }
        }
        else
        {
            fits_in_byte = true;
            value_r = value_l;
        }

        ++current_bucket_index;

        return true;
    }

    /** Вернуть текущий номер ячейки и соответствующее содержание.
      */
    inline std::pair<BucketIndex, UInt8> get() const
    {
        if ((current_bucket_index == 0) || is_eof)
            throw Exception("No available data.", ErrorCodes::NO_AVAILABLE_DATA);

        if (fits_in_byte)
            return std::make_pair(current_bucket_index - 1, locus.read(value_l));
        else
            return std::make_pair(current_bucket_index - 1, locus.read(value_l, value_r));
    }

private:
    ReadBuffer & in;
    /// Физическое расположение текущей ячейки.
    Locus locus;
    /// Текущая позиция в файле в виде номера ячейки.
    BucketIndex current_bucket_index = 0;
    /// Количество прочитанных байтов.
    size_t read_count = 0;
    /// Содержание в текущей позиции.
    UInt8 value_l;
    UInt8 value_r;
    ///
    bool is_eof = false;
    /// Влезает ли ячейка полностью в один байт?
    bool fits_in_byte;
};

/** Структура Locus содержит необходимую информацию, чтобы найти для каждой ячейки
  * соответствующие байт и смещение, в битах, от начала ячейки. Поскольку в общем
  * случае размер одного байта не делится на размер одной ячейки, возможны случаи,
  * когда одна ячейка перекрывает два байта. Поэтому структура Locus содержит две
  * пары (индекс, смещение).
  */
template <typename BucketIndex, UInt8 content_width, size_t bucket_count>
class CompactArray<BucketIndex, content_width, bucket_count>::Locus final
{
    friend class CompactArray;
    friend class CompactArray::Reader;

public:
    ALWAYS_INLINE operator UInt8() const
    {
        if (content_l == content_r)
            return read(*content_l);
        else
            return read(*content_l, *content_r);
    }

    Locus ALWAYS_INLINE & operator=(UInt8 content)
    {
        if ((index_l == index_r) || (index_l == (BITSET_SIZE - 1)))
        {
            /// Ячейка полностью влезает в один байт.
            *content_l &= ~(((1 << content_width) - 1) << offset_l);
            *content_l |= content << offset_l;
        }
        else
        {
            /// Ячейка перекрывает два байта.
            size_t left = 8 - offset_l;

            *content_l &= ~(((1 << left) - 1) << offset_l);
            *content_l |= (content & ((1 << left) - 1)) << offset_l;

            *content_r &= ~((1 << offset_r) - 1);
            *content_r |= content >> left;
        }

        return *this;
    }

private:
    Locus() = default;

    Locus(BucketIndex bucket_index)
    {
        init(bucket_index);
    }

    void ALWAYS_INLINE init(BucketIndex bucket_index)
    {
        size_t l = static_cast<size_t>(bucket_index) * content_width;
        index_l = l >> 3;
        offset_l = l & 7;

        size_t r = static_cast<size_t>(bucket_index + 1) * content_width;
        index_r = r >> 3;
        offset_r = r & 7;
    }

    UInt8 ALWAYS_INLINE read(UInt8 value_l) const
    {
        /// Ячейка полностью влезает в один байт.
        return (value_l >> offset_l) & ((1 << content_width) - 1);
    }

    UInt8 ALWAYS_INLINE read(UInt8 value_l, UInt8 value_r) const
    {
        /// Ячейка перекрывает два байта.
        return ((value_l >> offset_l) & ((1 << (8 - offset_l)) - 1))
            | ((value_r & ((1 << offset_r) - 1)) << (8 - offset_l));
    }

private:
    size_t index_l;
    size_t offset_l;
    size_t index_r;
    size_t offset_r;

    UInt8 * content_l;
    UInt8 * content_r;

    /// Проверки
    static_assert((content_width > 0) && (content_width < 8), "Invalid parameter value");
    static_assert(bucket_count <= (std::numeric_limits<size_t>::max() / content_width), "Invalid parameter value");
};

}

