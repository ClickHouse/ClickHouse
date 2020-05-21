#pragma once

class CudaHostPinnedMemPoolUtils
{
public:
    static size_t CalculatePadding(void * base_address, size_t alignment)
    {
        const size_t base = reinterpret_cast<size_t>(base_address);
        const size_t multiplier = (base / alignment) + 1;
        const size_t alignedAddress = multiplier * alignment;
        const size_t padding = alignedAddress - base;
        return padding;
    }

    static size_t CalculatePaddingWithHeader(void * base_address, size_t alignment, size_t header_size)
    {
        size_t padding = CalculatePadding(base_address, alignment);
        size_t needed_space = header_size;

        if (padding < needed_space)
        {
            // Header does not fit - Calculate next aligned address that header fits
            needed_space -= padding;

            // How many alignments I need to fit the header
            if (needed_space % alignment > 0)
                padding += alignment * (1 + (needed_space / alignment));
            else
                padding += alignment * (needed_space / alignment);
        }

        return padding;
    }
};
