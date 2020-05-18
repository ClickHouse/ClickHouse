#pragma once

#include <string.h>
#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#include <ext/bit_cast.h>
#include <Core/Types.h>
#include <Core/Defines.h>
#include <Common/PODArray.h>
#include <Columns/ColumnsCommon.h>

#include "oclBasics.h"
#include "bitonicSortKernels.cl"

class BitonicSort
{
public:
    using KernelType = OCL::KernelType;

    enum Types
    {
        KernelInt8 = 0,
        KernelUInt8,
        KernelInt16,
        KernelUInt16,
        KernelInt32,
        KernelUInt32,
        KernelInt64,
        KernelUInt64,
        KernelMax
    };

    static BitonicSort & getInstance()
    {
        static BitonicSort instance = BitonicSort();
        return instance;
    }

    /// Sorts given array in specified order. Returns `true` if given sequence was sorted, `false` otherwise.
    template <typename T>
    bool sort(const DB::PaddedPODArray<T> & data, DB::IColumn::Permutation & res, cl_uint sort_ascending [[maybe_unused]]) const
    {
        if constexpr (
            std::is_same_v<T, Int8> ||
            std::is_same_v<T, UInt8> ||
            std::is_same_v<T, Int16> ||
            std::is_same_v<T, UInt16> ||
            std::is_same_v<T, Int32> ||
            std::is_same_v<T, UInt32> ||
            std::is_same_v<T, Int64> ||
            std::is_same_v<T, UInt64>)
        {
            size_t data_size = data.size();

            /// Getting the nearest power of 2.
            size_t power = 8;
            while (power < data_size)
                power <<= 1;

            /// Allocates more space for additional stubs to be added if needed.
            std::vector<T> pairs_content(power);
            std::vector<UInt32> pairs_indices(power);

            memcpy(&pairs_content[0], &data[0], sizeof(T) * data_size);
            for (UInt32 i = 0; i < data_size; ++i)
                pairs_indices[i] = i;

            fillWithStubs(pairs_content.data(), pairs_indices.data(), data_size, power - data_size, sort_ascending);
            sort(pairs_content.data(), pairs_indices.data(), power, sort_ascending);

            for (size_t i = 0, shift = 0; i < power; ++i)
            {
                if (pairs_indices[i] >= data_size)
                {
                    ++shift;
                    continue;
                }
                res[i - shift] = pairs_indices[i];
            }

            return true;
        }

        return false;
    }

    /// Creating a configuration instance with making all OpenCl required variables
    /// such as device, platform, context, queue, program and kernel.
    void configure()
    {
        OCL::Settings settings = OCL::Settings(1, nullptr, 1, nullptr, 1, 0);

        cl_platform_id platform = OCL::getPlatformID(settings);
        cl_device_id device = OCL::getDeviceID(platform, settings);
        cl_context gpu_context = OCL::makeContext(device, settings);
        cl_command_queue command_queue = OCL::makeCommandQueue<2>(device, gpu_context, settings);

        cl_program program = OCL::makeProgram(bitonic_sort_kernels, gpu_context, device, settings);

        /// Creating kernels for each specified data type.
        cl_int error = 0;
        kernels.resize(KernelMax);

        kernels[KernelInt8] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_char", &error), clReleaseKernel);
        OCL::checkError(error);

        kernels[KernelUInt8] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_uchar", &error), clReleaseKernel);
        OCL::checkError(error);

        kernels[KernelInt16] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_short", &error), clReleaseKernel);
        OCL::checkError(error);

        kernels[KernelUInt16] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_ushort", &error), clReleaseKernel);
        OCL::checkError(error);

        kernels[KernelInt32] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_int", &error), clReleaseKernel);
        OCL::checkError(error);

        kernels[KernelUInt32] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_uint", &error), clReleaseKernel);
        OCL::checkError(error);

        kernels[KernelInt64] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_long", &error), clReleaseKernel);
        OCL::checkError(error);

        kernels[KernelUInt64] = std::shared_ptr<KernelType>(clCreateKernel(program, "bitonicSort_ulong", &error), clReleaseKernel);
        OCL::checkError(error);

        configuration = std::shared_ptr<OCL::Configuration>(new OCL::Configuration(device, gpu_context, command_queue, program));
    }

private:
    /// Dictionary with kernels for each type from list: uchar, char, ushort, short, uint, int, ulong and long.
    std::vector<std::shared_ptr<KernelType>> kernels;
    /// Current configuration with core OpenCL instances.
    std::shared_ptr<OCL::Configuration> configuration = nullptr;

    cl_kernel getKernel(Int8) const { return kernels[KernelInt8].get(); }
    cl_kernel getKernel(UInt8) const { return kernels[KernelUInt8].get(); }
    cl_kernel getKernel(Int16) const { return kernels[KernelInt16].get(); }
    cl_kernel getKernel(UInt16) const { return kernels[KernelUInt16].get(); }
    cl_kernel getKernel(Int32) const { return kernels[KernelInt32].get(); }
    cl_kernel getKernel(UInt32) const { return kernels[KernelUInt32].get(); }
    cl_kernel getKernel(Int64) const { return kernels[KernelInt64].get(); }
    cl_kernel getKernel(UInt64) const { return kernels[KernelUInt64].get(); }

    /// Sorts p_input inplace with indices. Works only with arrays which size equals to power of two.
    template <class T>
    void sort(T * p_input, cl_uint * indices, cl_int array_size, cl_uint sort_ascending) const
    {
        cl_kernel kernel = getKernel(T(0));
        cl_int error = CL_SUCCESS;
        cl_int num_stages = 0;

        for (cl_int temp = array_size; temp > 2; temp >>= 1)
            num_stages++;

        /// Creating OpenCL buffers using input arrays memory.
        cl_mem cl_input_buffer = OCL::createBuffer<T>(p_input, array_size, configuration.get()->context());
        cl_mem cl_indices_buffer = OCL::createBuffer<cl_uint>(indices, array_size, configuration.get()->context());

        configureKernel<cl_mem>(kernel, 0, static_cast<void *>(&cl_input_buffer));
        configureKernel<cl_mem>(kernel, 1, static_cast<void *>(&cl_indices_buffer));
        configureKernel<cl_uint>(kernel, 4, static_cast<void *>(&sort_ascending));

        for (cl_int stage = 0; stage < num_stages; stage++)
        {
            configureKernel<cl_uint>(kernel, 2, static_cast<void *>(&stage));

            for (cl_int pass_of_stage = stage; pass_of_stage >= 0; pass_of_stage--)
            {
                configureKernel<cl_uint>(kernel, 3, static_cast<void *>(&pass_of_stage));

                /// Setting work-item dimensions.
                size_t gsize = array_size / (2 * 4);
                size_t global_work_size[1] = {pass_of_stage ? gsize : gsize << 1 }; // number of quad items in input array

                /// Executing kernel.
                error = clEnqueueNDRangeKernel(configuration.get()->commandQueue(), kernel, 1, nullptr,
                                               global_work_size, nullptr, 0, nullptr, nullptr);
                OCL::checkError(error);
            }
        }

        /// Syncs all threads.
        OCL::finishCommandQueue(configuration.get()->commandQueue());

        OCL::releaseData(p_input, array_size, cl_input_buffer, configuration.get()->commandQueue());
        OCL::releaseData(indices, array_size, cl_indices_buffer, configuration.get()->commandQueue());
    }

    template <class T>
    void configureKernel(cl_kernel kernel, int number_of_argument, void * source) const
    {
        cl_int error = clSetKernelArg(kernel, number_of_argument, sizeof(T), source);
        OCL::checkError(error);
    }

    /// Fills given sequences from `arraySize` index with `numberOfStubs` values.
    template <class T>
    void fillWithStubs(T * p_input, cl_uint * indices, cl_int array_size, cl_int number_of_stubs, cl_uint sort_ascending) const
    {
        T value = sort_ascending ? std::numeric_limits<T>::max() : std::numeric_limits<T>::min();
        for (cl_int index = 0; index < number_of_stubs; ++index)
        {
            p_input[array_size + index] = value;
            indices[array_size + index] = array_size + index;
        }
    }

    BitonicSort() = default;
    BitonicSort(BitonicSort const &) = delete;
    void operator = (BitonicSort const &) = delete;
};
