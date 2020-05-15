// Copyright (c) 2009-2011 Intel Corporation
// All rights reserved.
//
// WARRANTY DISCLAIMER
//
// THESE MATERIALS ARE PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL INTEL OR ITS
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
// OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THESE
// MATERIALS, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// Intel Corporation is the author of the Materials, and requests that all
// problem reports or change requests be submitted to it directly

static const char * bitonic_sort_kernels = R"(

uchar4 makeMask_uchar(uchar4 srcLeft, uchar4 srcRight, uint4 indexLeft, uint4 indexRight)
{
    uchar4 mask = convert_uchar4(srcLeft < srcRight);

    if (srcLeft.x == srcRight.x)
        mask.x = (indexLeft.x < indexRight.x) ? UCHAR_MAX : 0;
    if (srcLeft.y == srcRight.y)
        mask.y = (indexLeft.y < indexRight.y) ? UCHAR_MAX : 0;
    if (srcLeft.z == srcRight.z)
        mask.z = (indexLeft.z < indexRight.z) ? UCHAR_MAX : 0;
    if (srcLeft.w == srcRight.w)
        mask.w = (indexLeft.w < indexRight.w) ? UCHAR_MAX : 0;

    return mask;
}


char4 makeMask_char(char4 srcLeft, char4 srcRight, uint4 indexLeft, uint4 indexRight)
{
    char4 mask = srcLeft < srcRight;

    if (srcLeft.x == srcRight.x)
        mask.x = -(indexLeft.x < indexRight.x);
    if (srcLeft.y == srcRight.y)
        mask.y = -(indexLeft.y < indexRight.y);
    if (srcLeft.z == srcRight.z)
        mask.z = -(indexLeft.z < indexRight.z);
    if (srcLeft.w == srcRight.w)
        mask.w = -(indexLeft.w < indexRight.w);

    return mask;
}


ushort4 makeMask_ushort(ushort4 srcLeft, ushort4 srcRight, uint4 indexLeft, uint4 indexRight)
{
    ushort4 mask = convert_ushort4(srcLeft < srcRight);

    if (srcLeft.x == srcRight.x)
        mask.x = (indexLeft.x < indexRight.x) ? USHRT_MAX : 0;
    if (srcLeft.y == srcRight.y)
        mask.y = (indexLeft.y < indexRight.y) ? USHRT_MAX : 0;
    if (srcLeft.z == srcRight.z)
        mask.z = (indexLeft.z < indexRight.z) ? USHRT_MAX : 0;
    if (srcLeft.w == srcRight.w)
        mask.w = (indexLeft.w < indexRight.w) ? USHRT_MAX : 0;

    return mask;
}


short4 makeMask_short(short4 srcLeft, short4 srcRight, uint4 indexLeft, uint4 indexRight)
{
    short4 mask = srcLeft < srcRight;

    if (srcLeft.x == srcRight.x)
        mask.x = -(indexLeft.x < indexRight.x);
    if (srcLeft.y == srcRight.y)
        mask.y = -(indexLeft.y < indexRight.y);
    if (srcLeft.z == srcRight.z)
        mask.z = -(indexLeft.z < indexRight.z);
    if (srcLeft.w == srcRight.w)
        mask.w = -(indexLeft.w < indexRight.w);

    return mask;
}


uint4 makeMask_uint(uint4 srcLeft, uint4 srcRight, uint4 indexLeft, uint4 indexRight)
{
    uint4 mask = convert_uint4(srcLeft < srcRight);

    if (srcLeft.x == srcRight.x)
        mask.x = (indexLeft.x < indexRight.x) ? UINT_MAX : 0;
    if (srcLeft.y == srcRight.y)
        mask.y = (indexLeft.y < indexRight.y) ? UINT_MAX : 0;
    if (srcLeft.z == srcRight.z)
        mask.z = (indexLeft.z < indexRight.z) ? UINT_MAX : 0;
    if (srcLeft.w == srcRight.w)
        mask.w = (indexLeft.w < indexRight.w) ? UINT_MAX : 0;

    return mask;
}


int4 makeMask_int(int4 srcLeft, int4 srcRight, uint4 indexLeft, uint4 indexRight)
{
    int4 mask = srcLeft < srcRight;

    if (srcLeft.x == srcRight.x)
        mask.x = -(indexLeft.x < indexRight.x);
    if (srcLeft.y == srcRight.y)
        mask.y = -(indexLeft.y < indexRight.y);
    if (srcLeft.z == srcRight.z)
        mask.z = -(indexLeft.z < indexRight.z);
    if (srcLeft.w == srcRight.w)
        mask.w = -(indexLeft.w < indexRight.w);

    return mask;
}


ulong4 makeMask_ulong(ulong4 srcLeft, ulong4 srcRight, uint4 indexLeft, uint4 indexRight)
{
    ulong4 mask = convert_ulong4(srcLeft < srcRight);

    if (srcLeft.x == srcRight.x)
        mask.x = (indexLeft.x < indexRight.x) ? ULONG_MAX : 0;
    if (srcLeft.y == srcRight.y)
        mask.y = (indexLeft.y < indexRight.y) ? ULONG_MAX : 0;
    if (srcLeft.z == srcRight.z)
        mask.z = (indexLeft.z < indexRight.z) ? ULONG_MAX : 0;
    if (srcLeft.w == srcRight.w)
        mask.w = (indexLeft.w < indexRight.w) ? ULONG_MAX : 0;

    return mask;
}


long4 makeMask_long(long4 srcLeft, long4 srcRight, uint4 indexLeft, uint4 indexRight)
{
    long4 mask = srcLeft < srcRight;

    if (srcLeft.x == srcRight.x)
        mask.x = -(indexLeft.x < indexRight.x);
    if (srcLeft.y == srcRight.y)
        mask.y = -(indexLeft.y < indexRight.y);
    if (srcLeft.z == srcRight.z)
        mask.z = -(indexLeft.z < indexRight.z);
    if (srcLeft.w == srcRight.w)
        mask.w = -(indexLeft.w < indexRight.w);

    return mask;
}


__kernel void __attribute__((vec_type_hint(char4))) __attribute__((vec_type_hint(uint4))) bitonicSort_char(__global char4 * theArray,
                         __global uint4 * indices,
                         const uint stage,
                         const uint passOfStage,
                         const uint dir)
{
    size_t i = get_global_id(0);
    char4 srcLeft, srcRight, mask;
    char4 imask10 = (char4)(0,  0, -1, -1);
    char4 imask11 = (char4)(0, -1,  0, -1);
    uint4 indexLeft, indexRight;

    if (stage > 0)
    {
        if (passOfStage > 0)    // upper level pass, exchange between two fours
        {
            size_t r = 1 << (passOfStage - 1); // length of arrow
            size_t lmask = r - 1;
            size_t left = ((i >> (passOfStage - 1)) << passOfStage) + (i & lmask);
            size_t right = left + r;

            srcLeft = theArray[left];
            srcRight = theArray[right];
            indexLeft = indices[left];
            indexRight = indices[right];

            mask = makeMask_char(srcLeft, srcRight, indexLeft, indexRight);

            char4 imin = (srcLeft & mask) | (srcRight & ~mask);
            char4 imax = (srcLeft & ~mask) | (srcRight & mask);

            if (((i >> (stage - 1)) & 1) ^ dir)
            {
                theArray[left]  = imin;
                theArray[right] = imax;

                indices[left] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[right] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
            else
            {
                theArray[right] = imin;
                theArray[left]  = imax;

                indices[right] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[left] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
        else    // last pass, sort inside one four
        {
            srcLeft = theArray[i];
            srcRight = srcLeft.zwxy;
            indexLeft = indices[i];
            indexRight = indexLeft.zwxy;

            mask = makeMask_char(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

            if (((i >> stage) & 1) ^ dir)
            {
                srcLeft = (srcLeft & mask) | (srcRight & ~mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_char(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
                indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            }
            else
            {
                srcLeft = (srcLeft & ~mask) | (srcRight & mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_char(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
                indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
    }
    else    // first stage, sort inside one four
    {
        char4 imask0 = (char4)(0, -1, -1,  0);

        srcLeft = theArray[i];
        srcRight = srcLeft.yxwz;
        indexLeft = indices[i];
        indexRight = indexLeft.yxwz;

        mask = makeMask_char(srcLeft, srcRight, indexLeft, indexRight) ^ imask0;

        if (dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        } else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }

        srcRight = srcLeft.zwxy;
        indexRight = indexLeft.zwxy;
        mask = makeMask_char(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

        if ((i & 1) ^ dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_char(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
            indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_char(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
            indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }
    }
}


__kernel void /*__attribute__((vec_type_hint(uchar4)))*/ bitonicSort_uchar(__global uchar4 * theArray,
                         __global uint4 * indices,
                         const uint stage,
                         const uint passOfStage,
                         const uint dir)
{
    size_t i = get_global_id(0);
    uchar4 srcLeft, srcRight, mask;
    uchar4 imask10 = (uchar4)(0,  0, -1, -1);
    uchar4 imask11 = (uchar4)(0, -1,  0, -1);
    uint4 indexLeft, indexRight;

    if (stage > 0)
    {
        if (passOfStage > 0)    // upper level pass, exchange between two fours
        {
            size_t r = 1 << (passOfStage - 1); // length of arrow
            size_t lmask = r - 1;
            size_t left = ((i >> (passOfStage - 1)) << passOfStage) + (i & lmask);
            size_t right = left + r;

            srcLeft = theArray[left];
            srcRight = theArray[right];
            indexLeft = indices[left];
            indexRight = indices[right];

            mask = makeMask_uchar(srcLeft, srcRight, indexLeft, indexRight);

            uchar4 imin = (srcLeft & mask) | (srcRight & ~mask);
            uchar4 imax = (srcLeft & ~mask) | (srcRight & mask);

            if (((i >> (stage - 1)) & 1) ^ dir)
            {
                theArray[left]  = imin;
                theArray[right] = imax;

                indices[left] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[right] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
            else
            {
                theArray[right] = imin;
                theArray[left]  = imax;

                indices[right] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[left] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
        else    // last pass, sort inside one four
        {
            srcLeft = theArray[i];
            srcRight = srcLeft.zwxy;
            indexLeft = indices[i];
            indexRight = indexLeft.zwxy;

            mask = makeMask_uchar(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

            if (((i >> stage) & 1) ^ dir)
            {
                srcLeft = (srcLeft & mask) | (srcRight & ~mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_uchar(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
                indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            }
            else
            {
                srcLeft = (srcLeft & ~mask) | (srcRight & mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_uchar(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
                indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
    }
    else    // first stage, sort inside one four
    {
        uchar4 imask0 = (uchar4)(0, -1, -1,  0);

        srcLeft = theArray[i];
        srcRight = srcLeft.yxwz;
        indexLeft = indices[i];
        indexRight = indexLeft.yxwz;

        mask = makeMask_uchar(srcLeft, srcRight, indexLeft, indexRight) ^ imask0;

        if (dir) {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        } else {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }

        srcRight = srcLeft.zwxy;
        indexRight = indexLeft.zwxy;
        mask = makeMask_uchar(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

        if ((i & 1) ^ dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_uchar(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
            indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_uchar(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
            indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }
    }
}


__kernel void /*__attribute__((vec_type_hint(short4)))*/ bitonicSort_short(__global short4 * theArray,
                         __global uint4 * indices,
                         const uint stage,
                         const uint passOfStage,
                         const uint dir)
{
    size_t i = get_global_id(0);
    short4 srcLeft, srcRight, mask;
    short4 imask10 = (short4)(0,  0, -1, -1);
    short4 imask11 = (short4)(0, -1,  0, -1);
    uint4 indexLeft, indexRight;

    if (stage > 0)
    {
        if (passOfStage > 0)    // upper level pass, exchange between two fours
        {
            size_t r = 1 << (passOfStage - 1); // length of arrow
            size_t lmask = r - 1;
            size_t left = ((i >> (passOfStage - 1)) << passOfStage) + (i & lmask);
            size_t right = left + r;

            srcLeft = theArray[left];
            srcRight = theArray[right];
            indexLeft = indices[left];
            indexRight = indices[right];

            mask = makeMask_short(srcLeft, srcRight, indexLeft, indexRight);

            short4 imin = (srcLeft & mask) | (srcRight & ~mask);
            short4 imax = (srcLeft & ~mask) | (srcRight & mask);

            if (((i >> (stage - 1)) & 1) ^ dir)
            {
                theArray[left]  = imin;
                theArray[right] = imax;

                indices[left] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[right] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
            else
            {
                theArray[right] = imin;
                theArray[left]  = imax;

                indices[right] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[left] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
        else    // last pass, sort inside one four
        {
            srcLeft = theArray[i];
            srcRight = srcLeft.zwxy;
            indexLeft = indices[i];
            indexRight = indexLeft.zwxy;

            mask = makeMask_short(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

            if (((i >> stage) & 1) ^ dir)
            {
                srcLeft = (srcLeft & mask) | (srcRight & ~mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_short(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
                indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            }
            else
            {
                srcLeft = (srcLeft & ~mask) | (srcRight & mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_short(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
                indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
    }
    else    // first stage, sort inside one four
    {
        short4 imask0 = (short4)(0, -1, -1,  0);

        srcLeft = theArray[i];
        srcRight = srcLeft.yxwz;
        indexLeft = indices[i];
        indexRight = indexLeft.yxwz;

        mask = makeMask_short(srcLeft, srcRight, indexLeft, indexRight) ^ imask0;

        if (dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }

        srcRight = srcLeft.zwxy;
        indexRight = indexLeft.zwxy;
        mask = makeMask_short(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

        if ((i & 1) ^ dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_short(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
            indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_short(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
            indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }
    }
}


__kernel void /*__attribute__((vec_type_hint(ushort4)))*/ bitonicSort_ushort(__global ushort4 * theArray,
                         __global uint4 * indices,
                         const uint stage,
                         const uint passOfStage,
                         const uint dir)
{
    size_t i = get_global_id(0);
    ushort4 srcLeft, srcRight, mask;
    ushort4 imask10 = (ushort4)(0,  0, -1, -1);
    ushort4 imask11 = (ushort4)(0, -1,  0, -1);
    uint4 indexLeft, indexRight;

    if (stage > 0)
    {
        if (passOfStage > 0)    // upper level pass, exchange between two fours
        {
            size_t r = 1 << (passOfStage - 1); // length of arrow
            size_t lmask = r - 1;
            size_t left = ((i >> (passOfStage - 1)) << passOfStage) + (i & lmask);
            size_t right = left + r;

            srcLeft = theArray[left];
            srcRight = theArray[right];
            indexLeft = indices[left];
            indexRight = indices[right];

            mask = makeMask_ushort(srcLeft, srcRight, indexLeft, indexRight);

            ushort4 imin = (srcLeft & mask) | (srcRight & ~mask);
            ushort4 imax = (srcLeft & ~mask) | (srcRight & mask);

            if (((i >> (stage - 1)) & 1) ^ dir)
            {
                theArray[left]  = imin;
                theArray[right] = imax;

                indices[left] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[right] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
            else
            {
                theArray[right] = imin;
                theArray[left]  = imax;

                indices[right] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[left] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
        else    // last pass, sort inside one four
        {
            srcLeft = theArray[i];
            srcRight = srcLeft.zwxy;
            indexLeft = indices[i];
            indexRight = indexLeft.zwxy;

            mask = makeMask_ushort(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

            if (((i >> stage) & 1) ^ dir)
            {
                srcLeft = (srcLeft & mask) | (srcRight & ~mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_ushort(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
                indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            }
            else
            {
                srcLeft = (srcLeft & ~mask) | (srcRight & mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_ushort(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
                indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
    }
    else    // first stage, sort inside one four
    {
        ushort4 imask0 = (ushort4)(0, -1, -1,  0);

        srcLeft = theArray[i];
        srcRight = srcLeft.yxwz;
        indexLeft = indices[i];
        indexRight = indexLeft.yxwz;

        mask = makeMask_ushort(srcLeft, srcRight, indexLeft, indexRight) ^ imask0;

        if (dir) {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        } else {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }

        srcRight = srcLeft.zwxy;
        indexRight = indexLeft.zwxy;
        mask = makeMask_ushort(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

        if ((i & 1) ^ dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_ushort(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
            indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_ushort(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
            indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }
    }
}


__kernel void /*__attribute__((vec_type_hint(int4)))*/ bitonicSort_int(__global int4 * theArray,
                         __global uint4 * indices,
                         const uint stage,
                         const uint passOfStage,
                         const uint dir)
{

    size_t i = get_global_id(0);
    int4 srcLeft, srcRight, mask;
    int4 imask10 = (int4)(0,  0, -1, -1);
    int4 imask11 = (int4)(0, -1,  0, -1);
    uint4 indexLeft, indexRight;

    int random_id = (i * 5 + stage * 42 + passOfStage * 47 + dir * 88) % 100;

    if (stage > 0)
    {
        if (passOfStage > 0)    // upper level pass, exchange between two fours
        {
            size_t r = 1 << (passOfStage - 1); // length of arrow
            size_t lmask = r - 1;
            size_t left = ((i >> (passOfStage - 1)) << passOfStage) + (i & lmask);
            size_t right = left + r;

            srcLeft = theArray[left];
            srcRight = theArray[right];
            indexLeft = indices[left];
            indexRight = indices[right];

            mask = makeMask_int(srcLeft, srcRight, indexLeft, indexRight);

            int4 imin = (srcLeft & mask) | (srcRight & ~mask);
            int4 imax = (srcLeft & ~mask) | (srcRight & mask);

            if (((i >> (stage - 1)) & 1) ^ dir)
            {
                theArray[left]  = imin;
                theArray[right] = imax;

                indices[left] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[right] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
            else
            {
                theArray[right] = imin;
                theArray[left]  = imax;

                indices[right] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[left] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
        else    // last pass, sort inside one four
        {
            srcLeft = theArray[i];
            srcRight = srcLeft.zwxy;
            indexLeft = indices[i];
            indexRight = indexLeft.zwxy;

            mask = makeMask_int(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

            if (((i >> stage) & 1) ^ dir)
            {
                srcLeft = (srcLeft & mask) | (srcRight & ~mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_int(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
                indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            }
            else
            {
                srcLeft = (srcLeft & ~mask) | (srcRight & mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_int(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
                indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
    }
    else    // first stage, sort inside one four
    {
        int4 imask0 = (int4)(0, -1, -1,  0);

        srcLeft = theArray[i];
        srcRight = srcLeft.yxwz;
        indexLeft = indices[i];
        indexRight = indexLeft.yxwz;

        mask = makeMask_int(srcLeft, srcRight, indexLeft, indexRight) ^ imask0;

        if (dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }

        srcRight = srcLeft.zwxy;
        indexRight = indexLeft.zwxy;
        mask = makeMask_int(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

        if ((i & 1) ^ dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_int(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
            indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_int(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
            indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }
    }
}


__kernel void /*__attribute__((vec_type_hint(uint4)))*/ bitonicSort_uint(__global uint4 * theArray,
                         __global uint4 * indices,
                         const uint stage,
                         const uint passOfStage,
                         const uint dir)
{
    size_t i = get_global_id(0);
    uint4 srcLeft, srcRight, mask;
    uint4 imask10 = (uint4)(0,  0, -1, -1);
    uint4 imask11 = (uint4)(0, -1,  0, -1);
    uint4 indexLeft, indexRight;

    if (stage > 0)
    {
        if (passOfStage > 0)    // upper level pass, exchange between two fours
        {
            size_t r = 1 << (passOfStage - 1); // length of arrow
            size_t lmask = r - 1;
            size_t left = ((i >> (passOfStage - 1)) << passOfStage) + (i & lmask);
            size_t right = left + r;

            srcLeft = theArray[left];
            srcRight = theArray[right];
            indexLeft = indices[left];
            indexRight = indices[right];

            mask = makeMask_uint(srcLeft, srcRight, indexLeft, indexRight);

            uint4 imin = (srcLeft & mask) | (srcRight & ~mask);
            uint4 imax = (srcLeft & ~mask) | (srcRight & mask);

            if (((i >> (stage - 1)) & 1) ^ dir)
            {
                theArray[left]  = imin;
                theArray[right] = imax;

                indices[left] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[right] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
            else
            {
                theArray[right] = imin;
                theArray[left]  = imax;

                indices[right] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[left] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
        else    // last pass, sort inside one four
        {
            srcLeft = theArray[i];
            srcRight = srcLeft.zwxy;
            indexLeft = indices[i];
            indexRight = indexLeft.zwxy;

            mask = makeMask_uint(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

            if (((i >> stage) & 1) ^ dir)
            {
                srcLeft = (srcLeft & mask) | (srcRight & ~mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_uint(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
                indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            }
            else
            {
                srcLeft = (srcLeft & ~mask) | (srcRight & mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_uint(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
                indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
    }
    else    // first stage, sort inside one four
    {
        uint4 imask0 = (uint4)(0, -1, -1,  0);

        srcLeft = theArray[i];
        srcRight = srcLeft.yxwz;
        indexLeft = indices[i];
        indexRight = indexLeft.yxwz;

        mask = makeMask_uint(srcLeft, srcRight, indexLeft, indexRight) ^ imask0;

        if (dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }

        srcRight = srcLeft.zwxy;
        indexRight = indexLeft.zwxy;
        mask = makeMask_uint(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

        if ((i & 1) ^ dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_uint(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
            indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_uint(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
            indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }
    }
}


__kernel void /*__attribute__((vec_type_hint(long4)))*/ bitonicSort_long(__global long4 * theArray,
                         __global uint4 * indices,
                         const uint stage,
                         const uint passOfStage,
                         const uint dir)
{
    size_t i = get_global_id(0);
    long4 srcLeft, srcRight, mask;
    long4 imask10 = (long4)(0,  0, -1, -1);
    long4 imask11 = (long4)(0, -1,  0, -1);
    uint4 indexLeft, indexRight;

    if (stage > 0)
    {
        if (passOfStage > 0)    // upper level pass, exchange between two fours
        {
            size_t r = 1 << (passOfStage - 1); // length of arrow
            size_t lmask = r - 1;
            size_t left = ((i >> (passOfStage - 1)) << passOfStage) + (i & lmask);
            size_t right = left + r;

            srcLeft = theArray[left];
            srcRight = theArray[right];
            indexLeft = indices[left];
            indexRight = indices[right];

            mask = makeMask_long(srcLeft, srcRight, indexLeft, indexRight);

            long4 imin = (srcLeft & mask) | (srcRight & ~mask);
            long4 imax = (srcLeft & ~mask) | (srcRight & mask);

            if (((i >> (stage - 1)) & 1) ^ dir)
            {
                theArray[left]  = imin;
                theArray[right] = imax;

                indices[left] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[right] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
            else
            {
                theArray[right] = imin;
                theArray[left]  = imax;

                indices[right] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[left] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
        else    // last pass, sort inside one four
        {
            srcLeft = theArray[i];
            srcRight = srcLeft.zwxy;
            indexLeft = indices[i];
            indexRight = indexLeft.zwxy;

            mask = makeMask_long(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

            if (((i >> stage) & 1) ^ dir)
            {
                srcLeft = (srcLeft & mask) | (srcRight & ~mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_long(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
                indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            }
            else
            {
                srcLeft = (srcLeft & ~mask) | (srcRight & mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_long(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
                indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
    }
    else    // first stage, sort inside one four
    {
        long4 imask0 = (long4)(0, -1, -1,  0);

        srcLeft = theArray[i];
        srcRight = srcLeft.yxwz;
        indexLeft = indices[i];
        indexRight = indexLeft.yxwz;

        mask = makeMask_long(srcLeft, srcRight, indexLeft, indexRight) ^ imask0;

        if (dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }

        srcRight = srcLeft.zwxy;
        indexRight = indexLeft.zwxy;
        mask = makeMask_long(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

        if ((i & 1) ^ dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_long(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
            indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_long(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
            indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }
    }
}


__kernel void /*__attribute__((vec_type_hint(ulong4)))*/ bitonicSort_ulong(__global ulong4 * theArray,
                         __global uint4 * indices,
                         const uint stage,
                         const uint passOfStage,
                         const uint dir)
{
    size_t i = get_global_id(0);
    ulong4 srcLeft, srcRight, mask;
    ulong4 imask10 = (ulong4)(0,  0, -1, -1);
    ulong4 imask11 = (ulong4)(0, -1,  0, -1);
    uint4 indexLeft, indexRight;

    if (stage > 0)
    {
        if (passOfStage > 0)    // upper level pass, exchange between two fours
        {
            size_t r = 1 << (passOfStage - 1); // length of arrow
            size_t lmask = r - 1;
            size_t left = ((i >> (passOfStage - 1)) << passOfStage) + (i & lmask);
            size_t right = left + r;

            srcLeft = theArray[left];
            srcRight = theArray[right];
            indexLeft = indices[left];
            indexRight = indices[right];

            mask = makeMask_ulong(srcLeft, srcRight, indexLeft, indexRight);

            ulong4 imin = (srcLeft & mask) | (srcRight & ~mask);
            ulong4 imax = (srcLeft & ~mask) | (srcRight & mask);

            if (((i >> (stage - 1)) & 1) ^ dir)
            {
                theArray[left]  = imin;
                theArray[right] = imax;

                indices[left] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[right] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
            else
            {
                theArray[right] = imin;
                theArray[left]  = imax;

                indices[right] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indices[left] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
        else    // last pass, sort inside one four
        {
            srcLeft = theArray[i];
            srcRight = srcLeft.zwxy;
            indexLeft = indices[i];
            indexRight = indexLeft.zwxy;

            mask = makeMask_ulong(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

            if (((i >> stage) & 1) ^ dir)
            {
                srcLeft = (srcLeft & mask) | (srcRight & ~mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_ulong(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
                indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            }
            else
            {
                srcLeft = (srcLeft & ~mask) | (srcRight & mask);
                srcRight = srcLeft.yxwz;
                indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
                indexRight = indexLeft.yxwz;

                mask = makeMask_ulong(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

                theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
                indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            }
        }
    }
    else    // first stage, sort inside one four
    {
        ulong4 imask0 = (ulong4)(0, -1, -1,  0);

        srcLeft = theArray[i];
        srcRight = srcLeft.yxwz;
        indexLeft = indices[i];
        indexRight = indexLeft.yxwz;

        mask = makeMask_ulong(srcLeft, srcRight, indexLeft, indexRight) ^ imask0;

        if (dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }

        srcRight = srcLeft.zwxy;
        indexRight = indexLeft.zwxy;
        mask = makeMask_ulong(srcLeft, srcRight, indexLeft, indexRight) ^ imask10;

        if ((i & 1) ^ dir)
        {
            srcLeft = (srcLeft & mask) | (srcRight & ~mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_ulong(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & mask) | (srcRight & ~mask);
            indices[i] = (indexLeft & convert_uint4(mask)) | (indexRight & convert_uint4(~mask));
        }
        else
        {
            srcLeft = (srcLeft & ~mask) | (srcRight & mask);
            srcRight = srcLeft.yxwz;
            indexLeft = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
            indexRight = indexLeft.yxwz;

            mask = makeMask_ulong(srcLeft, srcRight, indexLeft, indexRight) ^ imask11;

            theArray[i] = (srcLeft & ~mask) | (srcRight & mask);
            indices[i] = (indexLeft & convert_uint4(~mask)) | (indexRight & convert_uint4(mask));
        }
    }
}

)";
