/*
 * Density benchmark
 *
 * Copyright (c) 2015, Guillaume Voirin
 * All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * 5/04/15 19:30
 */

#include "benchmark.h"

void density_benchmark_version() {
    printf("\nSingle threaded ");
    DENSITY_BENCHMARK_BOLD(printf("in-memory benchmark"));
    printf(" powered by ");
    DENSITY_BENCHMARK_BOLD(printf("Density %i.%i.%i\n", density_version_major(), density_version_minor(), density_version_revision()));
    printf("Copyright (C) 2015 Guillaume Voirin\n");
    printf("Built for %s (%s endian system, %u bits) using " DENSITY_BENCHMARK_COMPILER ", %s %s\n", DENSITY_BENCHMARK_PLATFORM_STRING, DENSITY_BENCHMARK_ENDIAN_STRING, (unsigned int) (8 * sizeof(void *)), DENSITY_BENCHMARK_COMPILER_VERSION, __DATE__, __TIME__);
}

void density_benchmark_client_usage() {
    printf("\n");
    DENSITY_BENCHMARK_BOLD(printf("Usage :\n"));
    printf("  benchmark [OPTIONS ?]... [FILE ?]\n\n");
    DENSITY_BENCHMARK_BOLD(printf("Available options :\n"));
    printf("  -[LEVEL]                          Test file using only the specified compression LEVEL\n");
    printf("                                    If unspecified, all algorithms are tested (default).\n");
    printf("                                    LEVEL can have the following values (as values become higher,\n");
    printf("                                    compression ratio increases and speed diminishes) :\n");
    printf("                                    0 = Copy (no compression)\n");
    printf("                                    1 = Chameleon algorithm\n");
    printf("                                    2 = Cheetah algorithm\n");
    printf("                                    3 = Lion algorithm\n");
    printf("  -c                                Compress only\n");
    printf("  -f                                Activate fuzzer mode (pseudorandom generated data)\n");
    printf("  -h                                Print data hashing informations\n\n");
    exit(EXIT_SUCCESS);
}

void density_benchmark_format_decimal(uint64_t number) {
    if (number < 1000) {
        printf("%"PRIu64, number);
        return;
    }
    density_benchmark_format_decimal(number / 1000);
    printf(",%03"PRIu64, number % 1000);
}

const char *density_benchmark_convert_state_to_text(DENSITY_STATE state) {
    switch (state) {
        case DENSITY_STATE_ERROR_DURING_PROCESSING:
            return "Error during processing";
        case DENSITY_STATE_ERROR_INPUT_BUFFER_TOO_SMALL:
            return "Input buffer is too small";
        case DENSITY_STATE_ERROR_OUTPUT_BUFFER_TOO_SMALL:
            return "Output buffer is too small";
        case DENSITY_STATE_ERROR_INVALID_CONTEXT:
            return "Invalid context";
        case DENSITY_STATE_ERROR_INVALID_ALGORITHM:
            return "Invalid algorithm";
        default:
            return "Unknown error";
    }
}

int main(int argc, char *argv[]) {
    density_benchmark_version();
    DENSITY_ALGORITHM start_mode = DENSITY_ALGORITHM_CHAMELEON;
    DENSITY_ALGORITHM end_mode = DENSITY_ALGORITHM_LION;
    bool compression_only = false;
    bool fuzzer = false;
    bool hash_info = false;
    char *file_path = NULL;

    if (argc <= 1)
        density_benchmark_client_usage();
    for (int count = 1; count < argc; count++) {
        if (argv[count][0] == '-') {
            switch (argv[count][1]) {
                case '1':
                    start_mode = DENSITY_ALGORITHM_CHAMELEON;
                    end_mode = DENSITY_ALGORITHM_CHAMELEON;
                    break;
                case '2':
                    start_mode = DENSITY_ALGORITHM_CHEETAH;
                    end_mode = DENSITY_ALGORITHM_CHEETAH;
                    break;
                case '3':
                    start_mode = DENSITY_ALGORITHM_LION;
                    end_mode = DENSITY_ALGORITHM_LION;
                    break;
                case 'c':
                    compression_only = true;
                    break;
                case 'f':
                    fuzzer = true;
                    break;
                case 'h':
                    hash_info = true;
                    break;
                default:
                    density_benchmark_client_usage();
            }
        } else
            file_path = argv[argc - 1];
    }

    uint8_t *in;
    uint8_t *out;
    uint_fast64_t uncompressed_size;
    uint_fast64_t memory_allocated;
    if (fuzzer) {
        srand((unsigned int) (time(NULL) * 14521937821257379531llu));
        uncompressed_size = (uint_fast64_t) (((uint64_t) (rand() * 100000000llu)) / RAND_MAX);
        memory_allocated = density_compress_safe_size(uncompressed_size);
        in = malloc(memory_allocated * sizeof(uint8_t));
        uint8_t value = (uint8_t) rand();
        for (unsigned int count = 0; count < uncompressed_size; count++) {
            if (!(rand() & 0xf))
                value += (uint8_t)rand();
            in[count] = value;
        }
        out = malloc(memory_allocated * sizeof(uint8_t));
    } else {
        // Open file and get infos
        FILE *file = fopen(file_path, "rb");
        if (file == NULL) {
            DENSITY_BENCHMARK_ERROR(printf("Error opening file %s.", file_path), false);
        }
        struct stat file_attributes;
        stat(file_path, &file_attributes);

        // Allocate memory and copy file to memory
        uncompressed_size = (uint_fast64_t) file_attributes.st_size;
        memory_allocated = density_compress_safe_size(uncompressed_size);
        in = malloc(memory_allocated * sizeof(uint8_t));
        size_t read = fread(in, sizeof(uint8_t), uncompressed_size, file);
        if(uncompressed_size != read) {
            DENSITY_BENCHMARK_ERROR(printf("Error reading file %s.", file_path), false);
        }
        fclose(file);
        out = malloc(memory_allocated * sizeof(uint8_t));
    }

    printf("Allocated ");
    density_benchmark_format_decimal(2 * memory_allocated);
    printf(" bytes of in-memory work space\n");

    uint64_t original_hash_1 = DENSITY_BENCHMARK_HASH_SEED_1;
    uint64_t original_hash_2 = DENSITY_BENCHMARK_HASH_SEED_2;
    spookyhash_128(in, uncompressed_size, &original_hash_1, &original_hash_2);

    printf("\n");
    for (DENSITY_ALGORITHM compression_mode = start_mode; compression_mode <= end_mode; compression_mode++) {
        // Print algorithm info
        switch (compression_mode) {
            case DENSITY_ALGORITHM_CHAMELEON:
                DENSITY_BENCHMARK_BLUE(DENSITY_BENCHMARK_BOLD(printf("Chameleon algorithm")));
                DENSITY_BENCHMARK_UNDERLINE(19);
                break;
            case DENSITY_ALGORITHM_CHEETAH:
                DENSITY_BENCHMARK_BLUE(DENSITY_BENCHMARK_BOLD(printf("Cheetah algorithm")));
                DENSITY_BENCHMARK_UNDERLINE(17);
                break;
            case DENSITY_ALGORITHM_LION:
                DENSITY_BENCHMARK_BLUE(DENSITY_BENCHMARK_BOLD(printf("Lion algorithm")));
                DENSITY_BENCHMARK_UNDERLINE(14);
                break;
        }
        fflush(stdout);

        // Pre-heat
        printf("\nUsing ");
        if (fuzzer) {
            DENSITY_BENCHMARK_BOLD(printf("generated data"));
        } else {
            printf("file ");
            DENSITY_BENCHMARK_BOLD(printf("%s", file_path));
        }
        printf(" copied in memory\n");
        if(hash_info) {
            printf("Uncompressed data hash is ");
            DENSITY_BENCHMARK_BOLD(printf("0x%" PRIx64 "%" PRIx64, original_hash_1, original_hash_2));
            printf("\n");
        }

        density_processing_result result = density_compress(in, uncompressed_size, out, memory_allocated, compression_mode);
        if (result.state) {
            DENSITY_BENCHMARK_ERROR(printf("During compress API returned error %i (%s).", result.state, density_benchmark_convert_state_to_text(result.state)), true);
        }
        const uint_fast64_t compressed_size = result.bytesWritten;

        uint64_t hash_1 = DENSITY_BENCHMARK_HASH_SEED_1;
        uint64_t hash_2 = DENSITY_BENCHMARK_HASH_SEED_2;
        if(hash_info) {
            spookyhash_128(out, compressed_size, &hash_1, &hash_2);
            printf("Compressed data hash is ");
            DENSITY_BENCHMARK_BOLD(printf("0x%" PRIx64 "%" PRIx64, hash_1, hash_2));
            printf("\n");
        }

        if (!compression_only) {
            memset(in, 0, memory_allocated);
            result = density_decompress(out, compressed_size, in, memory_allocated);
            if (result.state) {
                DENSITY_BENCHMARK_ERROR(printf("During decompress API returned error %i (%s).", result.state, density_benchmark_convert_state_to_text(result.state)), true);
            }
            if (result.bytesWritten != uncompressed_size) {
                DENSITY_BENCHMARK_ERROR(printf("Round-trip size differs from original size (");
                density_benchmark_format_decimal(result.bytesWritten);
                printf(" bytes against ");
                density_benchmark_format_decimal(uncompressed_size);
                printf(" bytes).");, true);
            }

            hash_1 = DENSITY_BENCHMARK_HASH_SEED_1;
            hash_2 = DENSITY_BENCHMARK_HASH_SEED_2;
            spookyhash_128(in, uncompressed_size, &hash_1, &hash_2);

            if(hash_info) {
                printf("Round-trip data hash is ");
                DENSITY_BENCHMARK_BOLD(printf("0x%" PRIx64 "%" PRIx64, hash_1, hash_2));
                printf("\n");
            }

            if(hash_1 != original_hash_1 || hash_2 != original_hash_2) {
                DENSITY_BENCHMARK_ERROR(printf("Uncompressed and round-trip data hashes do not match (");
                printf("0x%" PRIx64 "%" PRIx64, hash_1, hash_2);
                printf(" vs. ");
                printf("0x%" PRIx64 "%" PRIx64, original_hash_1, original_hash_2);
                printf(").");, true);
            }

            printf("Uncompressed and round-trip data hashes match. ");
        }
        printf("Starting main bench.\n");
        if (compression_only)
            printf("Compressing ");
        else
            printf("Round-tripping ");
        density_benchmark_format_decimal(uncompressed_size);
        printf(" bytes to ");
        density_benchmark_format_decimal(compressed_size);
        printf(" bytes (compression ratio ");
        DENSITY_BENCHMARK_BOLD(printf("%.2lf%%", (100.0 * compressed_size) / uncompressed_size));
        printf(" or ");
        DENSITY_BENCHMARK_BOLD(printf("%.3fx", (1.0 * uncompressed_size) / compressed_size));
        if (compression_only)
            printf(")\n");
        else
            printf(") and back\n");
        fflush(stdout);

        // Main benchmark
        unsigned int iterations = 0;
        double compress_time_high = 0.0;
        double compress_time_low = 60.0;
        double decompress_time_high = 0.0;
        double decompress_time_low = 60.0;
        double total_compress_time = 0.0;
        double total_decompress_time = 0.0;
        double total_time = 0.0;
        double decompress_speed = 0.0;
        double decompress_speed_low = 0.0;
        double decompress_speed_high = 0.0;
        double compress_time_elapsed = 0.0;
        double decompress_time_elapsed = 0.0;
        cputime_chronometer chrono;

        while (total_time <= 10.0) {
            ++iterations;

            cputime_chronometer_start(&chrono);
            density_compress(in, uncompressed_size, out, memory_allocated, compression_mode);
            compress_time_elapsed = cputime_chronometer_stop(&chrono);

            if (!compression_only) {
                cputime_chronometer_start(&chrono);
                density_decompress(out, compressed_size, in, memory_allocated);
                decompress_time_elapsed = cputime_chronometer_stop(&chrono);
            }

            total_compress_time += compress_time_elapsed;

            if (compress_time_elapsed < compress_time_low)
                compress_time_low = compress_time_elapsed;
            if (compress_time_elapsed > compress_time_high)
                compress_time_high = compress_time_elapsed;

            double compress_speed = ((1.0 * uncompressed_size * iterations) / (total_compress_time * 1000.0 * 1000.0));
            double compress_speed_low = ((1.0 * uncompressed_size) / (compress_time_high * 1000.0 * 1000.0));
            double compress_speed_high = ((1.0 * uncompressed_size) / (compress_time_low * 1000.0 * 1000.0));

            total_time += compress_time_elapsed;

            if (!compression_only) {
                total_decompress_time += decompress_time_elapsed;

                if (decompress_time_elapsed < decompress_time_low)
                    decompress_time_low = decompress_time_elapsed;
                if (decompress_time_elapsed > decompress_time_high)
                    decompress_time_high = decompress_time_elapsed;

                decompress_speed = ((1.0 * uncompressed_size * iterations) / (total_decompress_time * 1000.0 * 1000.0));
                decompress_speed_low = ((1.0 * uncompressed_size) / (decompress_time_high * 1000.0 * 1000.0));
                decompress_speed_high = ((1.0 * uncompressed_size) / (decompress_time_low * 1000.0 * 1000.0));

                total_time += decompress_time_elapsed;
            }

            DENSITY_BENCHMARK_BLUE(printf("\rCompress speed ");
            DENSITY_BENCHMARK_BOLD(printf("%.0lf MB/s", compress_speed)));
            printf(" (min %.0lf MB/s, max %.0lf MB/s, best %.4lfs) ", compress_speed_low, compress_speed_high, compress_time_low);

            if (!compression_only) {
                printf("<=> ");
                DENSITY_BENCHMARK_BLUE(printf("Decompress speed ");
                DENSITY_BENCHMARK_BOLD(printf("%.0lf MB/s", decompress_speed)));
                printf(" (min %.0lf MB/s, max %.0lf MB/s, best %.4lfs)    ", decompress_speed_low, decompress_speed_high, decompress_time_low);
            }
            fflush(stdout);
        }
        printf("\nRun time %.3lfs (%i iterations)\n\n", total_time, iterations);
    }

    free(in);
    free(out);

    printf("Allocated memory released.\n\n");

    return EXIT_SUCCESS;
}
