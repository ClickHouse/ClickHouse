/*************************  dispatch_example.cpp   ****************************
| Author:        Agner Fog
| Date created:  2012-05-30
| Last modified: 2014-07-23
| Version:       1.14
| Project:       vector classes
| Description:
| Example of CPU dispatching.
|
| # Example of compiling this with GCC compiler:
| # Compile dispatch_example.cpp five times for different instruction sets:
| g++ -O3 -msse2    -c dispatch_example.cpp -od2.o
| g++ -O3 -msse4.1  -c dispatch_example.cpp -od5.o
| g++ -O3 -mavx     -c dispatch_example.cpp -od7.o
| g++ -O3 -mavx2    -c dispatch_example.cpp -od8.o
| g++ -O3 -mavx512f -c dispatch_example.cpp -od9.o
| g++ -O3 -msse2 -otest instrset_detect.cpp d2.o d5.o d7.o d8.o d9.o
| ./test
|
| (c) Copyright 2012 - 2014 GNU General Public License http://www.gnu.org/licenses
\*****************************************************************************/

#include <stdio.h>

#define MAX_VECTOR_SIZE 512
#include "vectorclass.h"


// define function type (change this to fit your purpose. Should not contain vector types)
typedef float MyFuncType(float*);

// function prototypes for each version
MyFuncType  myfunc, myfunc_SSE2, myfunc_SSE41, myfunc_AVX, myfunc_AVX2, myfunc_AVX512, myfunc_dispatch; 

// Define function name depending on which instruction set we compile for
#if   INSTRSET == 2                    // SSE2
#define FUNCNAME myfunc_SSE2
#elif INSTRSET == 5                    // SSE4.1
#define FUNCNAME myfunc_SSE41
#elif INSTRSET == 7                    // AVX
#define FUNCNAME myfunc_AVX
#elif INSTRSET == 8                    // AVX2
#define FUNCNAME myfunc_AVX2
#elif INSTRSET == 9                    // AVX512
#define FUNCNAME myfunc_AVX512
#endif

// specific version of the function. Compile once for each version
float FUNCNAME (float * f) {
    Vec16f a;                          // vector of 16 floats
    a.load(f);                         // load array into vector
    return horizontal_add(a);          // return sum of 16 elements
}


#if INSTRSET == 2
// make dispatcher in only the lowest of the compiled versions

// Function pointer initially points to the dispatcher.
// After first call it points to the selected version
MyFuncType * myfunc_pointer = &myfunc_dispatch;            // function pointer

// Dispatcher
float myfunc_dispatch(float * f) {
    int iset = instrset_detect();                          // Detect supported instruction set
    if      (iset >= 9) myfunc_pointer = &myfunc_AVX512;   // AVX512 version
    else if (iset >= 8) myfunc_pointer = &myfunc_AVX2;     // AVX2 version
    else if (iset >= 7) myfunc_pointer = &myfunc_AVX;      // AVX version
    else if (iset >= 5) myfunc_pointer = &myfunc_SSE41;    // SSE4.1 version
    else if (iset >= 2) myfunc_pointer = &myfunc_SSE2;     // SSE2 version
    else {
        // Error: lowest instruction set not supported (put your own error message here:)
        fprintf(stderr, "\nError: Instruction set SSE2 not supported on this computer");
        return 0.f;
    }
    // continue in dispatched version
    return (*myfunc_pointer)(f);
}


// Entry to dispatched function call
inline float myfunc(float * f) {
    return (*myfunc_pointer)(f);                           // go to dispatched version
}


// Example: main calls myfunc
int main(int argc, char* argv[]) 
{
    float a[16]={1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};  // array of 16 floats

    float sum = myfunc(a);                                 // call function with dispatching

    printf("\nsum = %8.3f \n", sum);                       // print result
    return 0;
}

#endif  // INSTRSET == 2

