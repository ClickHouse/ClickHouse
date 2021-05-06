/*
 * compress_functions.c
 * Copyright  : Kyle Harper
 * License    : Follows same licensing as the lizard_compress.c/lizard_compress.h program at any given time.  Currently, BSD 2.
 * Description: A program to demonstrate the various compression functions involved in when using Lizard_compress_MinLevel().  The idea
 *              is to show how each step in the call stack can be used directly, if desired.  There is also some benchmarking for
 *              each function to demonstrate the (probably lack of) performance difference when jumping the stack.
 *              (If you're new to lizard, please read simple_buffer.c to understand the fundamentals)
 *
 *              The call stack (before theoretical compiler optimizations) for Lizard_compress_MinLevel is as follows:
 *                Lizard_compress_MinLevel
 *                  Lizard_compress_fast
 *                    Lizard_compress_extState_MinLevel
 *                      Lizard_compress_generic
 *
 *              Lizard_compress_MinLevel()
 *                This is the recommended function for compressing data.  It will serve as the baseline for comparison.
 *              Lizard_compress_fast()
 *                Despite its name, it's not a "fast" version of compression.  It simply decides if HEAPMODE is set and either
 *                allocates memory on the heap for a struct or creates the struct directly on the stack.  Stack access is generally
 *                faster but this function itself isn't giving that advantage, it's just some logic for compile time.
 *              Lizard_compress_extState_MinLevel()
 *                This simply accepts all the pointers and values collected thus far and adds logic to determine how
 *                Lizard_compress_generic should be invoked; specifically: can the source fit into a single pass as determined by
 *                Lizard_64Klimit.
 *              Lizard_compress_generic()
 *                As the name suggests, this is the generic function that ultimately does most of the heavy lifting.  Calling this
 *                directly can help avoid some test cases and branching which might be useful in some implementation-specific
 *                situations, but you really need to know what you're doing AND what you're asking lizard to do!  You also need a
 *                wrapper function because this function isn't exposed with lizard_compress.h.
 *
 *              The call stack for decompression functions is shallow.  There are 2 options:
 *                Lizard_decompress_safe  ||  Lizard_decompress_fast
 *                  Lizard_decompress_generic
 *
 *               Lizard_decompress_safe
 *                 This is the recommended function for decompressing data.  It is considered safe because the caller specifies
 *                 both the size of the compresssed buffer to read as well as the maximum size of the output (decompressed) buffer
 *                 instead of just the latter.
 *               Lizard_decompress_generic
 *                 This is the generic function that both of the Lizard_decompress_* functions above end up calling.  Calling this
 *                 directly is not advised, period.  Furthermore, it is a static inline function in lizard_compress.c, so there isn't a symbol
 *                 exposed for anyone using lizard_compress.h to utilize.
 *
 *               Special Note About Decompression:
 *               Using the Lizard_decompress_safe() function protects against malicious (user) input. 
 */

/* Since lizard compiles with c99 and not gnu/std99 we need to enable POSIX linking for time.h structs and functions. */
#if __STDC_VERSION__ >= 199901L
#define _XOPEN_SOURCE 600
#else
#define _XOPEN_SOURCE 500
#endif
#define _POSIX_C_SOURCE 199309L

/* Includes, for Power! */
#include "lizard_compress.h"
#include "lizard_decompress.h"
#include <stdio.h>    /* for printf() */
#include <stdlib.h>   /* for exit() */
#include <string.h>   /* for atoi() memcmp() */
#include <stdint.h>   /* for uint_types */
#include <inttypes.h> /* for PRIu64 */
#include <time.h>     /* for clock_gettime() */
#include <locale.h>   /* for setlocale() */

/* We need to know what one billion is for clock timing. */
#define BILLION 1000000000L

/* Create a crude set of test IDs so we can switch on them later  (Can't switch() on a char[] or char*). */
#define ID__LIZARD_COMPRESS_DEFAULT        1
#define ID__LIZARD_COMPRESS_GENERIC        4
#define ID__LIZARD_DECOMPRESS_SAFE         5



/*
 * Easy show-error-and-bail function.
 */
void run_screaming(const char *message, const int code) {
  printf("%s\n", message);
  exit(code);
  return;
}


/*
 * Centralize the usage function to keep main cleaner.
 */
void usage(const char *message) {
  printf("Usage: ./argPerformanceTesting <iterations>\n");
  run_screaming(message, 1);
  return;
}



/*
 * Runs the benchmark for Lizard_compress_* based on function_id.
 */
uint64_t bench(
    const char *known_good_dst,
    const int function_id,
    const int iterations,
    const char *src,
    char *dst,
    const size_t src_size,
    const size_t max_dst_size,
    const size_t comp_size
  ) {
  uint64_t time_taken = 0;
  int rv = 0;
  const int warm_up = 5000;
  struct timespec start, end;
  Lizard_stream_t* state = Lizard_createStream_MinLevel();
  if (!state) return;

  // Select the right function to perform the benchmark on.  We perform 5000 initial loops to warm the cache and ensure that dst
  // remains matching to known_good_dst between successive calls.
  switch(function_id) {
    case ID__LIZARD_COMPRESS_DEFAULT:
      printf("Starting benchmark for function: Lizard_compress_MinLevel()\n");
      for(int junk=0; junk<warm_up; junk++)
        rv = Lizard_compress_MinLevel(src, dst, src_size, max_dst_size);
      if (rv < 1)
        run_screaming("Couldn't run Lizard_compress_MinLevel()... error code received is in exit code.", rv);
      if (memcmp(known_good_dst, dst, max_dst_size) != 0)
        run_screaming("According to memcmp(), the compressed dst we got doesn't match the known_good_dst... ruh roh.", 1);
      clock_gettime(CLOCK_MONOTONIC, &start);
      for (int i=1; i<=iterations; i++)
        Lizard_compress_MinLevel(src, dst, src_size, max_dst_size);
      break;

//    Disabled until Lizard_compress_generic() is exposed in the header.
//    case ID__LIZARD_COMPRESS_GENERIC:
//      printf("Starting benchmark for function: Lizard_compress_generic()\n");
//      Lizard_resetStream_MinLevel((Lizard_stream_t*)state);
//      for(int junk=0; junk<warm_up; junk++) {
//        Lizard_resetStream_MinLevel((Lizard_stream_t*)state);
//        //rv = Lizard_compress_generic_wrapper(state, src, dst, src_size, max_dst_size, notLimited, byU16, noDict, noDictIssue);
//        Lizard_compress_generic_wrapper(state, src, dst, src_size, max_dst_size);
//      }
//      if (rv < 1)
//        run_screaming("Couldn't run Lizard_compress_generic()... error code received is in exit code.", rv);
//      if (memcmp(known_good_dst, dst, max_dst_size) != 0)
//        run_screaming("According to memcmp(), the compressed dst we got doesn't match the known_good_dst... ruh roh.", 1);
//      for (int i=1; i<=iterations; i++) {
//        Lizard_resetStream_MinLevel((Lizard_stream_t*)state);
//        //Lizard_compress_generic_wrapper(state, src, dst, src_size, max_dst_size, notLimited, byU16, noDict, noDictIssue, acceleration);
//        Lizard_compress_generic_wrapper(state, src, dst, src_size, max_dst_size, acceleration);
//      }
//      break;

    case ID__LIZARD_DECOMPRESS_SAFE:
      printf("Starting benchmark for function: Lizard_decompress_safe()\n");
      for(int junk=0; junk<warm_up; junk++)
        rv = Lizard_decompress_safe(src, dst, comp_size, src_size);
      if (rv < 1)
        run_screaming("Couldn't run Lizard_decompress_safe()... error code received is in exit code.", rv);
      if (memcmp(known_good_dst, dst, src_size) != 0)
        run_screaming("According to memcmp(), the compressed dst we got doesn't match the known_good_dst... ruh roh.", 1);
      clock_gettime(CLOCK_MONOTONIC, &start);
      for (int i=1; i<=iterations; i++)
        Lizard_decompress_safe(src, dst, comp_size, src_size);
      break;

    default:
      run_screaming("The test specified isn't valid.  Please check your code.", 1);
      break;
  }

  // Stop timer and return time taken.
  clock_gettime(CLOCK_MONOTONIC, &end);
  time_taken = BILLION *(end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;

  Lizard_freeStream(state);
  return time_taken;
}



/*
 * main()
 * We will demonstrate the use of each function for simplicity sake.  Then we will run 2 suites of benchmarking:
 * Test suite A)  Uses generic Lorem Ipsum text which should be generally compressible insomuch as basic human text is
 *                compressible for such a small src_size
 * Test Suite B)  For the sake of testing, see what results we get if the data is drastically easier to compress.  IF there are
 *                indeed losses and IF more compressible data is faster to process, this will exacerbate the findings.
 */
int main(int argc, char **argv) {
  // Get and verify options.  There's really only 1:  How many iterations to run.
  int iterations = 1000000;
  if (argc > 1)
    iterations = atoi(argv[1]);
  if (iterations < 1)
    usage("Argument 1 (iterations) must be > 0.");

  // First we will create 2 sources (char *) of 2000 bytes each.  One normal text, the other highly-compressible text.
  const char *src    = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed luctus purus et risus vulputate, et mollis orci ullamcorper. Nulla facilisi. Fusce in ligula sed purus varius aliquet interdum vitae justo. Proin quis diam velit. Nulla varius iaculis auctor. Cras volutpat, justo eu dictum pulvinar, elit sem porttitor metus, et imperdiet metus sapien et ante. Nullam nisi nulla, ornare eu tristique eu, dignissim vitae diam. Nulla sagittis porta libero, a accumsan felis sagittis scelerisque.  Integer laoreet eleifend congue. Etiam rhoncus leo vel dolor fermentum, quis luctus nisl iaculis. Praesent a erat sapien. Aliquam semper mi in lorem ultrices ultricies. Lorem ipsum dolor sit amet, consectetur adipiscing elit. In feugiat risus sed enim ultrices, at sodales nulla tristique. Maecenas eget pellentesque justo, sed pellentesque lectus. Fusce sagittis sit amet elit vel varius. Donec sed ligula nec ligula vulputate rutrum sed ut lectus. Etiam congue pharetra leo vitae cursus. Morbi enim ante, porttitor ut varius vel, tincidunt quis justo. Nunc iaculis, risus id ultrices semper, metus est efficitur ligula, vel posuere risus nunc eget purus. Ut lorem turpis, condimentum at sem sed, porta aliquam turpis. In ut sapien a nulla dictum tincidunt quis sit amet lorem. Fusce at est egestas, luctus neque eu, consectetur tortor. Phasellus eleifend ultricies nulla ac lobortis.  Morbi maximus quam cursus vehicula iaculis. Maecenas cursus vel justo ut rutrum. Curabitur magna orci, dignissim eget dapibus vitae, finibus id lacus. Praesent rhoncus mattis augue vitae bibendum. Praesent porta mauris non ultrices fermentum. Quisque vulputate ipsum in sodales pulvinar. Aliquam nec mollis felis. Donec vitae augue pulvinar, congue nisl sed, pretium purus. Fusce lobortis mi ac neque scelerisque semper. Pellentesque vel est vitae magna aliquet aliquet. Nam non dolor. Nulla facilisi. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Morbi ac lacinia felis metus.";
  const char *hc_src = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  // Set and derive sizes.  Since we're using strings, use strlen() + 1 for \0.
  const size_t src_size = strlen(src) + 1;
  const size_t max_dst_size = Lizard_compressBound(src_size);
  int bytes_returned = 0;
  // Now build allocations for the data we'll be playing with.
  char *dst               = calloc(1, max_dst_size);
  char *known_good_dst    = calloc(1, max_dst_size);
  char *known_good_hc_dst = calloc(1, max_dst_size);
  if (dst == NULL || known_good_dst == NULL || known_good_hc_dst == NULL)
    run_screaming("Couldn't allocate memory for the destination buffers.  Sad :(", 1);

  // Create known-good buffers to verify our tests with other functions will produce the same results.
  bytes_returned = Lizard_compress_MinLevel(src, known_good_dst, src_size, max_dst_size);
  if (bytes_returned < 1)
    run_screaming("Couldn't create a known-good destination buffer for comparison... this is bad.", 1);
  const size_t src_comp_size = bytes_returned;
  bytes_returned = Lizard_compress_MinLevel(hc_src, known_good_hc_dst, src_size, max_dst_size);
  if (bytes_returned < 1)
    run_screaming("Couldn't create a known-good (highly compressible) destination buffer for comparison... this is bad.", 1);
  const size_t hc_src_comp_size = bytes_returned;


  /* Lizard_compress_MinLevel() */
  // This is the default function so we don't need to demonstrate how to use it.  See basics.c if you need more basal information.

  /* Lizard_compress_extState_MinLevel() */
  // Using this function directly requires that we build an Lizard_stream_t struct ourselves.  We do NOT have to reset it ourselves.
  memset(dst, 0, max_dst_size);
  Lizard_stream_t* state = Lizard_createStream_MinLevel();
  if (!state) return;
  bytes_returned = Lizard_compress_extState_MinLevel(state, src, dst, src_size, max_dst_size, 1);
  if (bytes_returned < 1)
    run_screaming("Failed to compress src using Lizard_compress_extState_MinLevel.  echo $? for return code.", bytes_returned);
  if (memcmp(dst, known_good_dst, bytes_returned) != 0)
    run_screaming("According to memcmp(), the value we got in dst from Lizard_compress_extState_MinLevel doesn't match the known-good value.  This is bad.", 1);

  /* Lizard_compress_generic */
  // When you can exactly control the inputs and options of your Lizard needs, you can use Lizard_compress_generic and fixed (const)
  // values for the enum types such as dictionary and limitations.  Any other direct-use is probably a bad idea.
  //
  // That said, the Lizard_compress_generic() function is 'static inline' and does not have a prototype in lizard_compress.h to expose a symbol
  // for it.  In other words: we can't access it directly.  I don't want to submit a PR that modifies lizard_compress.c/h.  Yann and others can
  // do that if they feel it's worth expanding this example.
  //
  // I will, however, leave a skeleton of what would be required to use it directly:
  /*
    memset(dst, 0, max_dst_size);
    // Lizard_stream_t state:  is already declared above.  We can reuse it BUT we have to reset the stream ourselves between each call.
    Lizard_resetStream_MinLevel((Lizard_stream_t *)state);
    // Since src size is small we know the following enums will be used:  notLimited (0), byU16 (2), noDict (0), noDictIssue (0).
    bytes_returned = Lizard_compress_generic(state, src, dst, src_size, max_dst_size, notLimited, byU16, noDict, noDictIssue, 1);
    if (bytes_returned < 1)
      run_screaming("Failed to compress src using Lizard_compress_generic.  echo $? for return code.", bytes_returned);
    if (memcmp(dst, known_good_dst, bytes_returned) != 0)
      run_screaming("According to memcmp(), the value we got in dst from Lizard_compress_generic doesn't match the known-good value.  This is bad.", 1);
  */
  Lizard_freeStream(state);


  /* Benchmarking */
  /* Now we'll run a few rudimentary benchmarks with each function to demonstrate differences in speed based on the function used.
   * Remember, we cannot call Lizard_compress_generic() directly (yet) so it's disabled.
   */
  // Suite A - Normal Compressibility
  char *dst_d = calloc(1, src_size);
  memset(dst, 0, max_dst_size);
  printf("\nStarting suite A:  Normal compressible text.\n");
  uint64_t time_taken__default       = bench(known_good_dst, ID__LIZARD_COMPRESS_DEFAULT,       iterations, src,            dst,   src_size, max_dst_size, src_comp_size);
  //uint64_t time_taken__generic       = bench(known_good_dst, ID__LIZARD_COMPRESS_GENERIC,       iterations, src,            dst,   src_size, max_dst_size, src_comp_size);
  uint64_t time_taken__decomp_safe   = bench(src,            ID__LIZARD_DECOMPRESS_SAFE,        iterations, known_good_dst, dst_d, src_size, max_dst_size, src_comp_size);
  // Suite B - Highly Compressible
  memset(dst, 0, max_dst_size);
  printf("\nStarting suite B:  Highly compressible text.\n");
  uint64_t time_taken_hc__default       = bench(known_good_hc_dst, ID__LIZARD_COMPRESS_DEFAULT,       iterations, hc_src,            dst,   src_size, max_dst_size, hc_src_comp_size);
  //uint64_t time_taken_hc__generic       = bench(known_good_hc_dst, ID__LIZARD_COMPRESS_GENERIC,       iterations, hc_src,            dst,   src_size, max_dst_size, hc_src_comp_size);
  uint64_t time_taken_hc__decomp_safe   = bench(hc_src,            ID__LIZARD_DECOMPRESS_SAFE,        iterations, known_good_hc_dst, dst_d, src_size, max_dst_size, hc_src_comp_size);

  // Report and leave.
  setlocale(LC_ALL, "");
  const char *format        = "|%-14s|%-30s|%'14.9f|%'16d|%'14d|%'13.2f%%|\n";
  const char *header_format = "|%-14s|%-30s|%14s|%16s|%14s|%14s|\n";
  const char *separator     = "+--------------+------------------------------+--------------+----------------+--------------+--------------+\n";
  printf("\n");
  printf("%s", separator);
  printf(header_format, "Source", "Function Benchmarked", "Total Seconds", "Iterations/sec", "ns/Iteration", "% of default");
  printf("%s", separator);
  printf(format, "Normal Text", "Lizard_compress_MinLevel()",       (double)time_taken__default       / BILLION, (int)(iterations / ((double)time_taken__default       /BILLION)), time_taken__default       / iterations, (double)time_taken__default       * 100 / time_taken__default);
  printf(format, "Normal Text", "Lizard_compress_fast()",          (double)time_taken__fast          / BILLION, (int)(iterations / ((double)time_taken__fast          /BILLION)), time_taken__fast          / iterations, (double)time_taken__fast          * 100 / time_taken__default);
  printf(format, "Normal Text", "Lizard_compress_extState_MinLevel()", (double)time_taken__fast_extstate / BILLION, (int)(iterations / ((double)time_taken__fast_extstate /BILLION)), time_taken__fast_extstate / iterations, (double)time_taken__fast_extstate * 100 / time_taken__default);
  //printf(format, "Normal Text", "Lizard_compress_generic()",       (double)time_taken__generic       / BILLION, (int)(iterations / ((double)time_taken__generic       /BILLION)), time_taken__generic       / iterations, (double)time_taken__generic       * 100 / time_taken__default);
  printf(format, "Normal Text", "Lizard_decompress_safe()",        (double)time_taken__decomp_safe   / BILLION, (int)(iterations / ((double)time_taken__decomp_safe   /BILLION)), time_taken__decomp_safe   / iterations, (double)time_taken__decomp_safe   * 100 / time_taken__default);
  printf(header_format, "", "", "", "", "", "");
  printf(format, "Compressible", "Lizard_compress_MinLevel()",       (double)time_taken_hc__default       / BILLION, (int)(iterations / ((double)time_taken_hc__default       /BILLION)), time_taken_hc__default       / iterations, (double)time_taken_hc__default       * 100 / time_taken_hc__default);
  printf(format, "Compressible", "Lizard_compress_fast()",          (double)time_taken_hc__fast          / BILLION, (int)(iterations / ((double)time_taken_hc__fast          /BILLION)), time_taken_hc__fast          / iterations, (double)time_taken_hc__fast          * 100 / time_taken_hc__default);
  printf(format, "Compressible", "Lizard_compress_extState_MinLevel()", (double)time_taken_hc__fast_extstate / BILLION, (int)(iterations / ((double)time_taken_hc__fast_extstate /BILLION)), time_taken_hc__fast_extstate / iterations, (double)time_taken_hc__fast_extstate * 100 / time_taken_hc__default);
  //printf(format, "Compressible", "Lizard_compress_generic()",       (double)time_taken_hc__generic       / BILLION, (int)(iterations / ((double)time_taken_hc__generic       /BILLION)), time_taken_hc__generic       / iterations, (double)time_taken_hc__generic       * 100 / time_taken_hc__default);
  printf(format, "Compressible", "Lizard_decompress_safe()",        (double)time_taken_hc__decomp_safe   / BILLION, (int)(iterations / ((double)time_taken_hc__decomp_safe   /BILLION)), time_taken_hc__decomp_safe   / iterations, (double)time_taken_hc__decomp_safe   * 100 / time_taken_hc__default);
  printf("%s", separator);
  printf("\n");
  printf("All done, ran %d iterations per test.\n", iterations);
  return 0;
}
