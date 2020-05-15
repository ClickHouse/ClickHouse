select toInt8(number * 2) as x from numbers(42) order by x desc settings special_sort = 'opencl_bitonic'
