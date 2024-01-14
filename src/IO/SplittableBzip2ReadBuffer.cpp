#include "SplittableBzip2ReadBuffer.h"
#include <IO/SeekableReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const Int32 LOGICAL_ERROR;
    extern const Int32 POSITION_OUT_OF_BOUND;
}

std::vector<Int32> & SplittableBzip2ReadBuffer::Data::initTT(Int32 length)
{
    if (tt.size() < static_cast<size_t>(length))
        tt.resize(length);
    return tt;
}

template <typename T>
std::string SplittableBzip2ReadBuffer::Data::arrayToString(const std::vector<T> & arr)
{
    std::string result = "[";
    for (size_t i = 0; i < arr.size(); i++)
    {
        if (i)
            result += ", ";

        result += std::to_string(static_cast<Int32>(arr[i]));
    }
    result += "]";
    return result;
}

template <typename T>
std::string SplittableBzip2ReadBuffer::Data::array2DToString(T arr[BZip2Constants::N_GROUPS][BZip2Constants::MAX_ALPHA_SIZE])
{
    std::string result = "[";
    for (int i = 0; i < BZip2Constants::N_GROUPS; i++)
    {
        if (i)
            result += ", ";

        result += arrayToString(arr[i], BZip2Constants::MAX_ALPHA_SIZE);
    }
    result += "]";
    return result;
}

template <typename T>
std::string SplittableBzip2ReadBuffer::Data::arrayToString(const T * arr, size_t size)
{
    std::string result = "[";
    for (size_t i = 0; i < size; i++)
    {
        if (i)
            result += ", ";

        if constexpr (std::is_same_v<T, bool>)
            result += arr[i] ? "true" : "false";
        else
            result += std::to_string(static_cast<Int32>(arr[i]));
    }
    result += "]";
    return result;
}

std::string SplittableBzip2ReadBuffer::Data::toString()
{
    std::string result = "Data{";
    result += "\ninUse=" + arrayToString(inUse, 256);
    result += "\nseqToUnseq=" + arrayToString(seqToUnseq, 256);
    result += "\nselector=" + arrayToString(selector, BZip2Constants::MAX_SELECTORS);
    result += "\nselectorMtf=" + arrayToString(selectorMtf, BZip2Constants::MAX_SELECTORS);
    result += "\nunzftab=" + arrayToString(unzftab, 256);
    result += "\nlimit=" + array2DToString(limit);
    result += "\nbase=" + array2DToString(base);
    result += "\nperm=" + array2DToString(perm);
    result += "\nminLens=" + arrayToString(minLens, BZip2Constants::N_GROUPS);
    result += "\ncftab=" + arrayToString(cftab, 257);
    result += "\ngetAndMoveToFrontDecode_yy=" + arrayToString(getAndMoveToFrontDecode_yy, 256);
    result += "\ntemp_charArray2d=" + array2DToString(temp_charArray2d);
    result += "\nrecvDecodingTables_pos=" + arrayToString(recvDecodingTables_pos, BZip2Constants::N_GROUPS);
    result += "\ntt=" + arrayToString(tt);
    result += "\nll8=" + arrayToString(ll8);
    result += "}";
    return result;
}


void SplittableBzip2ReadBuffer::hbCreateDecodeTables(
    int * __restrict limit,
    int * __restrict base,
    int * __restrict perm,
    const UInt16 * __restrict length,
    int minLen,
    int maxLen,
    int alphaSize)
{
    for (int i = minLen, pp = 0; i <= maxLen; i++)
    {
        for (int j = 0; j < alphaSize; j++)
            if (length[j] == i)
                perm[pp++] = j;
    }
    for (int i = BZip2Constants::MAX_CODE_LEN - 1; i > 0; --i)
    {
        base[i] = 0;
        limit[i] = 0;
    }

    for (int i = 0; i < alphaSize; i++)
        base[length[i] + 1]++;

    for (int i = 1, b = base[0]; i < BZip2Constants::MAX_CODE_LEN; i++)
    {
        b += base[i];
        base[i] = b;
    }

    for (int i = minLen, vec = 0, b = base[i]; i <= maxLen; i++)
    {
        int nb = base[i + 1];
        vec += nb - b;
        b = nb;
        limit[i] = vec - 1;
        vec <<= 1;
    }

    for (int i = minLen + 1; i <= maxLen; i++)
        base[i] = ((limit[i - 1] + 1) << 1) - base[i];
}

SplittableBzip2ReadBuffer::SplittableBzip2ReadBuffer(
    std::unique_ptr<ReadBuffer> in_, size_t buf_size, char * existing_memory, size_t alignment)
    : CompressedReadBufferWrapper(std::move(in_), buf_size, existing_memory, alignment)
    , blockSize100k(9)
    , currentState(STATE::NO_PROCESS_STATE)
    , skipResult(false)
    , currentChar(0)
    , storedBlockCRC(0)
    , blockRandomised(false)
    , data(nullptr)
    , computedBlockCRC(0)
    , storedCombinedCRC(0)
    , computedCombinedCRC(0)
    , origPtr(0)
    , nInUse(0)
    , bsBuff(0)
    , bsLive(0)
    , last(0)
{
    skipResult = skipToNextMarker(BLOCK_DELIMITER, DELIMITER_BIT_LENGTH);

    /// Update adjusted_start
    auto * seekable = dynamic_cast<SeekableReadBuffer*>(in.get());
    if (seekable && skipResult)
    {
        adjusted_start = seekable->getPosition();
    }
    changeStateToProcessABlock();
}

Int32 SplittableBzip2ReadBuffer::read(char * dest, size_t dest_size, size_t offs, size_t len)
{
    if (offs + len > dest_size)
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "offs({}) + len({}) > dest_size({}).", offs, len, dest_size);

    const size_t hi = offs + len;
    size_t destOffs = offs;
    Int32 b = 0;
    for (; (destOffs < hi && (b = read0()) >= 0); ++destOffs)
    {
        dest[destOffs] = static_cast<char>(b);
    }

    Int32 result = static_cast<Int32>(destOffs - offs);
    if (result == 0)
    {
        result = b;
        skipResult = skipToNextMarker(SplittableBzip2ReadBuffer::BLOCK_DELIMITER, DELIMITER_BIT_LENGTH);
        changeStateToProcessABlock();
    }
    return result;
}

bool SplittableBzip2ReadBuffer::nextImpl()
{
    Position dest = internal_buffer.begin();
    size_t dest_size = internal_buffer.size();
    size_t offset = 0;
    Int32 result;
    do
    {
        result = read(dest, dest_size, offset, dest_size - offset);
        if (result > 0)
            offset += result;
    } while (result != -1 && offset < dest_size);

    if (offset)
    {
        working_buffer.resize(offset);
        return true;
    }
    else
    {
        return false;
    }
}

Int32 SplittableBzip2ReadBuffer::read0()
{
    Int32 retChar = currentChar;

    switch (currentState)
    {
        case STATE::END_OF_FILE:
            return BZip2Constants::END_OF_STREAM;
        case STATE::NO_PROCESS_STATE:
            return BZip2Constants::END_OF_BLOCK;
        case STATE::START_BLOCK_STATE:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "wrong state {}", magic_enum::enum_name(currentState));
        case STATE::RAND_PART_A_STATE:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "wrong state {}", magic_enum::enum_name(currentState));
        case STATE::RAND_PART_B_STATE:
            setupRandPartB();
            break;
        case STATE::RAND_PART_C_STATE:
            setupRandPartC();
            break;
        case STATE::NO_RAND_PART_A_STATE:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "wrong state {}", magic_enum::enum_name(currentState));
        case STATE::NO_RAND_PART_B_STATE:
            setupNoRandPartB();
            break;
        case STATE::NO_RAND_PART_C_STATE:
            setupNoRandPartC();
            break;
    }
    return retChar;
}

Int32 SplittableBzip2ReadBuffer::readAByte(ReadBuffer & in_)
{
    char c;
    if (in_.read(c))
        return static_cast<Int32>(c) & 0xff;
    else
        return -1;
}

bool SplittableBzip2ReadBuffer::skipToNextMarker(Int64 marker, Int32 markerBitLength, ReadBuffer & in_, Int64 & bsBuff_, Int64 & bsLive_)
{
    try
    {
        if (markerBitLength > 63)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "skipToNextMarker can not find patterns greater than 63 bits");

        Int64 bytes = bsR(markerBitLength, in_, bsBuff_, bsLive_);
        if (bytes == -1)
        {
            return false;
        }

        while (true)
        {
            if (bytes == marker)
            {
                return true;
            }
            else
            {
                bytes = bytes << 1;
                bytes = bytes & ((1L << markerBitLength) - 1);
                Int32 oneBit = static_cast<Int32>(bsR(1, in_, bsBuff_, bsLive_));
                if (oneBit != -1)
                {
                    bytes = bytes | oneBit;
                }
                else
                {
                    return false;
                }
            }
        }
    }
    catch (Exception &)
    {
        return false;
    }
}

bool SplittableBzip2ReadBuffer::skipToNextMarker(Int64 marker, Int32 markerBitLength)
{
    return skipToNextMarker(marker, markerBitLength, *in, bsBuff, bsLive);
}

void SplittableBzip2ReadBuffer::makeMaps()
{
    Int32 nInUseShadow = 0;
    for (Int32 i = 0; i < 256; i++)
        if (data->inUse[i])
            data->seqToUnseq[nInUseShadow++] = i;
    nInUse = nInUseShadow;
}

void SplittableBzip2ReadBuffer::changeStateToProcessABlock()
{
    if (skipResult == true)
    {
        initBlock();
        setupBlock();
    }
    else
    {
        currentState = STATE::END_OF_FILE;
    }
}

void SplittableBzip2ReadBuffer::initBlock()
{
    storedBlockCRC = bsGetInt();
    blockRandomised = (bsR(1) == 1);

    if (!data)
        data = std::make_unique<Data>(blockSize100k);

    getAndMoveToFrontDecode();
    crc.initialiseCRC();
    currentState = STATE::START_BLOCK_STATE;
}

void SplittableBzip2ReadBuffer::endBlock()
{
    computedBlockCRC = crc.getFinalCRC();
    if (storedBlockCRC != computedBlockCRC)
    {
        computedCombinedCRC = (storedCombinedCRC << 1) | (static_cast<UInt32>(storedCombinedCRC) >> 31);
        computedCombinedCRC ^= storedBlockCRC;
        reportCRCError();
    }
    computedCombinedCRC = (computedCombinedCRC << 1) | (static_cast<UInt32>(computedCombinedCRC) >> 31);
    computedCombinedCRC ^= computedBlockCRC;
}

void SplittableBzip2ReadBuffer::complete()
{
    storedCombinedCRC = bsGetInt();
    currentState = STATE::END_OF_FILE;
    data = nullptr;
    if (storedCombinedCRC != computedCombinedCRC)
        reportCRCError();
}

Int64 SplittableBzip2ReadBuffer::bsR(Int64 n, ReadBuffer & in_, Int64 & bsBuff_, Int64 & bsLive_)
{
    Int64 bsLiveShadow = bsLive_;
    Int64 bsBuffShadow = bsBuff_;
    if (bsLiveShadow < n)
    {
        do
        {
            Int32 thech = readAByte(in_);
            if (thech < 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected end of stream");

            bsBuffShadow = (bsBuffShadow << 8) | thech;
            bsLiveShadow += 8;
        } while (bsLiveShadow < n);

        bsBuff_ = bsBuffShadow;
    }

    bsLive_ = bsLiveShadow - n;
    return (bsBuffShadow >> (bsLiveShadow - n)) & ((1L << n) - 1);
}

Int64 SplittableBzip2ReadBuffer::bsR(Int64 n)
{
    return bsR(n, *in, bsBuff, bsLive);
}

bool SplittableBzip2ReadBuffer::bsGetBit()
{
    Int64 bsLiveShadow = bsLive;
    Int64 bsBuffShadow = bsBuff;
    if (bsLiveShadow < 1)
    {
        Int32 thech = readAByte(*in);
        if (thech < 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected end of stream");

        bsBuffShadow = (bsBuffShadow << 8) | thech;
        bsLiveShadow += 8;
        bsBuff = bsBuffShadow;
    }
    bsLive = bsLiveShadow - 1;
    return ((bsBuffShadow >> (bsLiveShadow - 1)) & 1) != 0;
}

void SplittableBzip2ReadBuffer::recvDecodingTables()
{
    Data * dataShadow = data.get();
    bool * inUse = dataShadow->inUse;
    char * pos = dataShadow->recvDecodingTables_pos;
    char * selector = dataShadow->selector;
    char * selectorMtf = dataShadow->selectorMtf;

    Int32 inUse16 = 0;
    for (Int32 i = 0; i < 16; ++i)
        if (bsGetBit())
            inUse16 |= 1 << i;

    for (Int32 i = 255; i >= 0; --i)
        inUse[i] = false;

    for (Int32 i = 0; i < 16; ++i)
    {
        if ((inUse16 & (1 << i)) != 0)
        {
            Int32 i16 = i << 4;
            for (Int32 j = 0; j < 16; j++)
                if (bsGetBit())
                    inUse[i16 + j] = true;
        }
    }
    makeMaps();

    Int32 alphaSize = nInUse + 2;
    Int32 nGroups = static_cast<Int32>(bsR(3));
    Int32 nSelectors = static_cast<Int32>(bsR(15));
    for (Int32 i = 0; i < nSelectors; ++i)
    {
        Int32 j = 0;
        while (bsGetBit())
            j++;
        selectorMtf[i] = j;
    }

    for (Int32 v = nGroups - 1; v >= 0; --v)
        pos[v] = v;

    for (Int32 i = 0; i < nSelectors; ++i)
    {
        Int32 v = selectorMtf[i] & 0xff;
        char tmp = pos[v];
        while (v > 0)
        {
            pos[v] = pos[v - 1];
            v--;
        }
        pos[0] = tmp;
        selector[i] = tmp;
    }

    auto * len = dataShadow->temp_charArray2d;
    for (Int32 t = 0; t < nGroups; t++)
    {
        Int32 curr = static_cast<Int32>(bsR(5));
        auto * len_t = len[t];
        for (Int32 i = 0; i < alphaSize; i++)
        {
            while (bsGetBit())
                curr += bsGetBit() ? -1 : 1;
            len_t[i] = curr;
        }
    }

    createHuffmanDecodingTables(alphaSize, nGroups);
}

void SplittableBzip2ReadBuffer::createHuffmanDecodingTables(Int32 alphaSize, Int32 nGroups)
{
    Data * dataShadow = data.get();
    auto * len = dataShadow->temp_charArray2d;
    auto * minLens = dataShadow->minLens;
    auto * limit = dataShadow->limit;
    auto * base = dataShadow->base;
    auto * perm = dataShadow->perm;
    for (Int32 t = 0; t < nGroups; t++)
    {
        Int32 minLen = 32;
        Int32 maxLen = 0;
        auto * len_t = len[t];
        for (Int32 i = alphaSize - 1; i >= 0; --i)
        {
            Int32 lent = len_t[i];
            if (lent > maxLen)
                maxLen = lent;
            if (lent < minLen)
                minLen = lent;
        }
        hbCreateDecodeTables(limit[t], base[t], perm[t], len[t], minLen, maxLen, alphaSize);
        minLens[t] = minLen;
    }
}

void SplittableBzip2ReadBuffer::getAndMoveToFrontDecode()
{
    origPtr = static_cast<Int32>(bsR(24));
    recvDecodingTables();

    ReadBuffer * inShadow = in.get();
    Data * dataShadow = data.get();
    auto & ll8 = dataShadow->ll8;
    Int32 * unzftab = dataShadow->unzftab;
    char * selector = dataShadow->selector;
    auto * seqToUnseq = dataShadow->seqToUnseq;
    auto * yy = dataShadow->getAndMoveToFrontDecode_yy;
    Int32 * minLens = dataShadow->minLens;
    auto * limit = dataShadow->limit;
    auto * base = dataShadow->base;
    auto * perm = dataShadow->perm;
    Int32 limitLast = blockSize100k * 100000;

    for (Int32 i = 256; --i >= 0;)
    {
        yy[i] = i;
        unzftab[i] = 0;
    }

    Int32 groupNo = 0;
    Int32 groupPos = BZip2Constants::G_SIZE - 1;
    Int32 eob = nInUse + 1;
    Int32 nextSym = getAndMoveToFrontDecode0(0);
    Int32 bsBuffShadow = static_cast<Int32>(bsBuff);
    Int32 bsLiveShadow = static_cast<Int32>(bsLive);
    Int32 lastShadow = -1;
    Int32 zt = selector[groupNo] & 0xff;
    Int32 * base_zt = base[zt];
    Int32 * limit_zt = limit[zt];
    Int32 * perm_zt = perm[zt];
    Int32 minLens_zt = minLens[zt];

    while (nextSym != eob)
    {
        if ((nextSym == BZip2Constants::RUNA) || (nextSym == BZip2Constants::RUNB))
        {
            Int32 s = -1;
            for (Int32 n = 1; true; n <<= 1)
            {
                if (nextSym == BZip2Constants::RUNA)
                    s += n;
                else if (nextSym == BZip2Constants::RUNB)
                    s += n << 1;
                else
                    break;

                if (groupPos == 0)
                {
                    groupPos = BZip2Constants::G_SIZE - 1;
                    zt = selector[++groupNo] & 0xff;
                    base_zt = base[zt];
                    limit_zt = limit[zt];
                    perm_zt = perm[zt];
                    minLens_zt = minLens[zt];
                }
                else
                {
                    groupPos--;
                }

                Int32 zn = minLens_zt;
                while (bsLiveShadow < zn)
                {
                    Int32 thech = readAByte(*inShadow);
                    if (thech < 0)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected end of stream");

                    bsBuffShadow = (bsBuffShadow << 8) | thech;
                    bsLiveShadow += 8;
                }

                Int64 zvec = (bsBuffShadow >> (bsLiveShadow - zn)) & ((1 << zn) - 1);
                bsLiveShadow -= zn;
                while (zvec > limit_zt[zn])
                {
                    zn++;
                    while (bsLiveShadow < 1)
                    {
                        Int32 thech = readAByte(*inShadow);
                        if (thech < 0)
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected end of stream");

                        bsBuffShadow = (bsBuffShadow << 8) | thech;
                        bsLiveShadow += 8;
                    }
                    bsLiveShadow--;
                    zvec = (zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1);
                }
                nextSym = perm_zt[static_cast<Int32>(zvec - base_zt[zn])];
            }

            char ch = seqToUnseq[yy[0]];
            unzftab[ch & 0xff] += s + 1;
            while (s-- >= 0)
                ll8[++lastShadow] = ch;
            if (lastShadow >= limitLast)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "block overrun");
        }
        else
        {
            if (++lastShadow >= limitLast)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "block overrun");
            auto tmp = yy[nextSym - 1];
            unzftab[seqToUnseq[tmp] & 0xff]++;
            ll8[lastShadow] = seqToUnseq[tmp];

            if (nextSym <= 16)
                for (Int32 j = nextSym - 1; j > 0; --j)
                    yy[j] = yy[j - 1];
            else
                memmove(&yy[1], &yy[0], (nextSym - 1) * sizeof(yy[0]));
            yy[0] = tmp;

            if (groupPos == 0)
            {
                groupPos = BZip2Constants::G_SIZE - 1;
                zt = selector[++groupNo] & 0xff;
                base_zt = base[zt];
                limit_zt = limit[zt];
                perm_zt = perm[zt];
                minLens_zt = minLens[zt];
            }
            else
            {
                groupPos--;
            }

            Int32 zn = minLens_zt;
            while (bsLiveShadow < zn)
            {
                Int32 thech = readAByte(*inShadow);
                if (thech < 0)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected end of stream");

                bsBuffShadow = (bsBuffShadow << 8) | thech;
                bsLiveShadow += 8;
            }

            Int32 zvec = (bsBuffShadow >> (bsLiveShadow - zn)) & ((1 << zn) - 1);
            bsLiveShadow -= zn;
            while (zvec > limit_zt[zn])
            {
                zn++;
                while (bsLiveShadow < 1)
                {
                    Int32 thech = readAByte(*inShadow);
                    if (thech < 0)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected end of stream");

                    bsBuffShadow = (bsBuffShadow << 8) | thech;
                    bsLiveShadow += 8;
                }
                bsLiveShadow--;
                zvec = ((zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1));
            }
            nextSym = perm_zt[zvec - base_zt[zn]];
        }
    }

    last = lastShadow;
    bsLive = bsLiveShadow;
    bsBuff = bsBuffShadow;
}

Int32 SplittableBzip2ReadBuffer::getAndMoveToFrontDecode0(Int32 groupNo)
{
    ReadBuffer * inShadow = in.get();
    Data * dataShadow = data.get();
    Int32 zt = dataShadow->selector[groupNo] & 0xff;
    Int32 * limit_zt = dataShadow->limit[zt];
    Int32 zn = dataShadow->minLens[zt];
    Int32 zvec = static_cast<Int32>(bsR(zn));
    Int32 bsLiveShadow = static_cast<Int32>(bsLive);
    Int32 bsBuffShadow = static_cast<Int32>(bsBuff);
    while (zvec > limit_zt[zn])
    {
        zn++;
        while (bsLiveShadow < 1)
        {
            Int32 thech = readAByte(*inShadow);
            if (thech < 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "unexpected end of stream");

            bsBuffShadow = (bsBuffShadow << 8) | thech;
            bsLiveShadow += 8;
        }
        bsLiveShadow--;
        zvec = (zvec << 1) | ((bsBuffShadow >> bsLiveShadow) & 1);
    }

    bsLive = bsLiveShadow;
    bsBuff = bsBuffShadow;
    return dataShadow->perm[zt][zvec - dataShadow->base[zt][zn]];
}

void SplittableBzip2ReadBuffer::setupBlock()
{
    if (!data)
        return;

    Int32 * cftab = data->cftab;
    std::vector<Int32> & tt = data->initTT(last + 1);
    auto & ll8 = data->ll8;
    cftab[0] = 0;
    memcpy(&cftab[1], &data->unzftab[0], 256 * sizeof(cftab[0]));
    for (Int32 i = 1, c = cftab[0]; i <= 256; i++)
    {
        c += cftab[i];
        cftab[i] = c;
    }
    for (Int32 i = 0, lastShadow = last; i <= lastShadow; i++)
        tt[cftab[ll8[i] & 0xff]++] = i;

    if (origPtr < 0 || static_cast<size_t>(origPtr) >= tt.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "stream corrupted");

    su_tPos = tt[origPtr];
    su_count = 0;
    su_i2 = 0;
    su_ch2 = 256;
    if (blockRandomised)
    {
        su_rNToGo = 0;
        su_rTPos = 0;
        setupRandPartA();
    }
    else
    {
        setupNoRandPartA();
    }
}

void SplittableBzip2ReadBuffer::setupRandPartA()
{
    if (su_i2 <= last)
    {
        su_chPrev = su_ch2;
        Int32 su_ch2Shadow = data->ll8[su_tPos] & 0xff;
        su_tPos = data->tt[su_tPos];
        if (su_rNToGo == 0)
        {
            su_rNToGo = BZip2Constants::rNums[su_rTPos] - 1;
            if (++su_rTPos == 512)
                su_rTPos = 0;
        }
        else
        {
            su_rNToGo--;
        }
        su_ch2 = ((su_ch2Shadow ^= (su_rNToGo == 1)) ? 1 : 0);
        su_i2++;
        currentChar = su_ch2Shadow;
        currentState = STATE::RAND_PART_B_STATE;
        crc.updateCRC(su_ch2Shadow);
    }
    else
    {
        endBlock();
        currentState = STATE::NO_PROCESS_STATE;
    }
}

void SplittableBzip2ReadBuffer::setupNoRandPartA()
{
    if (su_i2 <= last)
    {
        su_chPrev = su_ch2;
        Int32 su_ch2Shadow = data->ll8[su_tPos] & 0xff;
        su_ch2 = su_ch2Shadow;
        su_tPos = data->tt[su_tPos];
        su_i2++;
        currentChar = su_ch2Shadow;
        currentState = STATE::NO_RAND_PART_B_STATE;
        crc.updateCRC(su_ch2Shadow);
    }
    else
    {
        currentState = STATE::NO_RAND_PART_A_STATE;
        endBlock();
        currentState = STATE::NO_PROCESS_STATE;
    }
}

void SplittableBzip2ReadBuffer::setupRandPartB()
{
    if (su_ch2 != su_chPrev)
    {
        currentState = STATE::RAND_PART_A_STATE;
        su_count = 1;
        setupRandPartA();
    }
    else if (++su_count >= 4)
    {
        su_z = static_cast<char>(data->ll8[su_tPos] & 0xff);
        su_tPos = data->tt[su_tPos];
        if (su_rNToGo == 0)
        {
            su_rNToGo = BZip2Constants::rNums[su_rTPos] - 1;
            if (++su_rTPos == 512)
                su_rTPos = 0;
        }
        else
        {
            su_rNToGo--;
        }
        su_j2 = 0;
        currentState = STATE::RAND_PART_C_STATE;
        if (su_rNToGo == 1)
            su_z ^= 1;
        setupRandPartC();
    }
    else
    {
        currentState = STATE::RAND_PART_A_STATE;
        setupRandPartA();
    }
}

void SplittableBzip2ReadBuffer::setupRandPartC()
{
    if (su_j2 < su_z)
    {
        currentChar = su_ch2;
        crc.updateCRC(su_ch2);
        su_j2++;
    }
    else
    {
        currentState = STATE::RAND_PART_A_STATE;
        su_i2++;
        su_count = 0;
        setupRandPartA();
    }
}

void SplittableBzip2ReadBuffer::setupNoRandPartB()
{
    if (su_ch2 != su_chPrev)
    {
        su_count = 1;
        setupNoRandPartA();
    }
    else if (++su_count >= 4)
    {
        su_z = static_cast<char>(data->ll8[su_tPos] & 0xff);
        su_tPos = data->tt[su_tPos];
        su_j2 = 0;
        setupNoRandPartC();
    }
    else
    {
        setupNoRandPartA();
    }
}

void SplittableBzip2ReadBuffer::setupNoRandPartC()
{
    if (su_j2 < su_z)
    {
        Int32 su_ch2Shadow = su_ch2;
        currentChar = su_ch2Shadow;
        crc.updateCRC(su_ch2Shadow);
        su_j2++;
        currentState = STATE::NO_RAND_PART_C_STATE;
    }
    else
    {
        su_i2++;
        su_count = 0;
        setupNoRandPartA();
    }
}

}
