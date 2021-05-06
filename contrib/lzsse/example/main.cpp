// LZSSE.cpp : Defines the entry point for the console application.
//
#define _CRT_SECURE_NO_WARNINGS 1

#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include "../lzsse2/lzsse2.h"
#include "../lzsse4/lzsse4.h"
#include "../lzsse8/lzsse8.h"

static const uint32_t MAGIC_NUMBER = 0x28F19732;

void DisplayUsage()
{
    printf( "Usage:\n" );
    printf( "    lzsse [args] input_file output_file\n" );
    printf( "\n" );
    printf( "Arguments:\n" );
    printf( "    -2    Compress in lzsse2 mode (default)\n" );
    printf( "    -4    Compress in lzsse4 mode\n" );
    printf( "    -8    Compress in lzsse8 mode\n" );
    printf( "    -f    Optimal parse (default)\n" );
    printf( "    -o    Fast parse (not available for lzsse2)\n" );
    printf( "    -d    Decompress\n" );
    printf( "    -lN   Compression level for optimal parse, where N is 1 to 17 (default 16)\n" );
    printf( "    -bN   Block size in KiB, default 131,072\n" );
    printf( "\n" );
}

static size_t CompressorFastLZSSE4( LZSSE4_FastParseState* state, const void* input, size_t inputLength, void* output, size_t outputLength, unsigned int )
{
    return LZSSE4_CompressFast( state, input, inputLength, output, outputLength );
}

static size_t CompressorFastLZSSE8( LZSSE8_FastParseState* state, const void* input, size_t inputLength, void* output, size_t outputLength, unsigned int )
{
    return LZSSE8_CompressFast( state, input, inputLength, output, outputLength );
}

template <typename State>
void Compress( FILE* inputFile, FILE* outputFile, uint64_t blockSize, uint8_t mode, unsigned int level, State* state, size_t (*compressor)( State*, const void*, size_t, void*, size_t, unsigned int ) )
{
    if ( state == nullptr )
    {
        printf( "Couldn't allocate parse state\n" );
        exit( 1 );
    }

    if ( fwrite( &MAGIC_NUMBER, sizeof( uint32_t ), 1, outputFile ) == 0 )
    {
        printf( "Couldn't write magic number\n" );
        exit( 1 );
    }

    if ( fwrite( &mode, sizeof( uint8_t ), 1, outputFile ) == 0 )
    {
        printf( "Couldn't write stream type\n" );
        exit( 1 );
    }

    if ( fwrite( &blockSize, sizeof( uint64_t ), 1, outputFile ) == 0 )
    {
        printf( "Couldn't write block size\n" );
        exit( 1 );
    }

    size_t   typedBlockSize = static_cast< size_t >( blockSize );
    uint8_t* inputBuffer    = reinterpret_cast< uint8_t* >( malloc( typedBlockSize ) );
    uint8_t* outputBuffer   = reinterpret_cast< uint8_t* >( malloc( typedBlockSize ) );

    if ( inputBuffer == nullptr || outputBuffer == nullptr )
    {
        printf( "Couldn't allocate buffer memory\n" );
        exit( 1 );
    }

    for ( ;; )
    {
        size_t readSize = fread( inputBuffer, 1, blockSize, inputFile );

        if ( readSize == 0 )
        {
            break;
        }

        size_t compressedSize = compressor( state, inputBuffer, readSize, outputBuffer, typedBlockSize, level );

        if ( compressedSize == 0 )
        {
            printf( "Compression function failed\n" );
            exit( 1 );
        }

        uint32_t compressedLength   = static_cast< uint32_t >( compressedSize );
        uint32_t uncompressedLength = static_cast< uint32_t >( readSize );

        if ( fwrite( &uncompressedLength, sizeof( uint32_t ), 1, outputFile ) < 1  )
        {
            break;
        }

        if ( fwrite( &compressedLength, sizeof( uint32_t ), 1, outputFile ) < 1 )
        {
            printf( "Error writing compressed length from block\n" );
            exit( 1 );
        }

        if ( fwrite( outputBuffer, 1, compressedLength, outputFile ) != compressedLength )
        {
            printf( "Error writing block\n" );
            exit( 1 );
        }
    }
    
    free( inputBuffer );
    free( outputBuffer );
}

void Decompress( FILE* inputFile, FILE* outputFile )
{
    uint32_t magicNumber;
    uint64_t blockSize     = 128 * 1024 * 1024;

    if ( fread( &magicNumber, sizeof( uint32_t ), 1, inputFile ) < 1 || magicNumber != MAGIC_NUMBER )
    {
        printf( "Couldn't read magic number, or magic number incorrect\n" );
        exit( 1 );
    }

    uint8_t streamType;

    if ( fread( &streamType, sizeof( uint8_t ), 1, inputFile ) < 1 )
    {
        printf( "Couldn't read stream type\n" );
        exit( 1 );
    }

    if ( fread( &blockSize, sizeof( uint64_t ), 1, inputFile ) < 1 )
    {
        printf( "Couldn't read block size\n" );
        exit( 1 );
    }

    uint8_t* inputBuffer  = reinterpret_cast< uint8_t* >( malloc( static_cast< size_t >( blockSize ) ) );
    uint8_t* outputBuffer = reinterpret_cast< uint8_t* >( malloc( static_cast< size_t >( blockSize ) ) );

    if ( inputBuffer == nullptr || outputBuffer == nullptr )
    {
        printf( "Couldn't allocate buffer memory\n" );
        exit( 1 );
    }

    size_t( *decompressor )( const void*, size_t, void*, size_t );

    switch ( streamType )
    {
    case 2:

        decompressor = LZSSE2_Decompress;
        break;

    case 4:

        decompressor = LZSSE4_Decompress;
        break;

    case 8:

        decompressor = LZSSE8_Decompress;
        break;

    default:

        printf( "Invalid stream type\n" );
        exit( 1 );

    }

    memset( inputBuffer, 0, blockSize );
    memset( outputBuffer, 0, blockSize );

    for ( ;; )
    {
        uint32_t compressedLength;
        uint32_t uncompressedLength;

        if ( fread( &uncompressedLength, sizeof( uint32_t ), 1, inputFile ) < 1  )
        {
            break;
        }

        if ( fread( &compressedLength, sizeof( uint32_t ), 1, inputFile ) < 1  )
        {
            printf( "Error reading compressed length from block\n" );
            exit( 1 );
        }

        if ( fread( inputBuffer, 1, compressedLength, inputFile ) != compressedLength )
        {
            printf( "Error reading block\n" );
            exit( 1 );
        }

        size_t decompressedSize = 0;
        
        decompressedSize =
           decompressor( inputBuffer,
                         compressedLength,
                         outputBuffer,
                         uncompressedLength );
        
        if ( decompressedSize != size_t( uncompressedLength ) )
        {
            printf( "Error in decompression stream\n" );
            exit( 1 );
        }

        if ( fwrite( outputBuffer, 1, uncompressedLength, outputFile ) != uncompressedLength )
        {
            printf( "Couldn't write block to output file\n" );
            exit( 1 );
        }
    }

    free( inputBuffer );
    free( outputBuffer );
}

int main( int argc, const char** argv )
{
    bool         decompression = false;
    bool         optimal       = true;
    uint64_t     blockSize     = 128 * 1024 * 1024;
    uint8_t      mode          = 2;
    unsigned int level         = 16;
    
    if ( argc < 3 )
    {
        DisplayUsage();
        exit( 1 );
    }
    
    for ( int argIndex = 1; argIndex < argc - 2; ++argIndex )
    {
        const char* arg = argv[ argIndex ];

        if ( arg[ 0 ] == '-' )
        {
            switch ( arg[ 1 ] )
            {
            case 'd':

                decompression = true;
                break;

            case '2':
                
                mode = 2;
                break;

            case '4':

                mode = 4;
                break;

            case '8':

                mode = 8;
                break;

            case 'l':

                level = static_cast< unsigned int >( strtoul( arg + 2, nullptr, 10 ) );
                break;

            case 'b':

                blockSize = strtoull( arg + 2, nullptr, 10 ) * 1024;
                break;

            case 'o':

                optimal = true;
                break;

            case 'f':

                optimal = false;
                break;

            }
        }
    }

    FILE* inputFile  = fopen( argv[ argc - 2 ], "rb" );

    if ( inputFile == nullptr )
    {
        perror( argv[ argc - 2 ] );
        exit( 1 );
    }

    FILE* outputFile = fopen( argv[ argc - 1 ], "wb+" );
    
    if ( outputFile == nullptr )
    {
        perror( argv[ argc - 2 ] );
        exit( 1 );
    }
 
    if ( decompression )
    {
        Decompress( inputFile, outputFile );
    }
    else
    {
        switch ( mode )
        {
        case 2:
        {
            LZSSE2_OptimalParseState* state = LZSSE2_MakeOptimalParseState( static_cast< size_t >( blockSize ) );

            Compress( inputFile, outputFile, blockSize, mode, level, state, LZSSE2_CompressOptimalParse );

            LZSSE2_FreeOptimalParseState( state );

            break;
        }

        case 4:
        {
            if ( optimal )
            {
                LZSSE4_OptimalParseState* state = LZSSE4_MakeOptimalParseState( static_cast<size_t>( blockSize ) );

                Compress( inputFile, outputFile, blockSize, mode, level, state, LZSSE4_CompressOptimalParse );

                LZSSE4_FreeOptimalParseState( state );
            }
            else
            {
                LZSSE4_FastParseState* state = LZSSE4_MakeFastParseState();

                Compress( inputFile, outputFile, blockSize, mode, level, state, CompressorFastLZSSE4 );

                LZSSE4_FreeFastParseState( state );
            }

            break;
        }
        
        case 8:
        {
            if ( optimal )
            {
                LZSSE8_OptimalParseState* state = LZSSE8_MakeOptimalParseState( static_cast<size_t>( blockSize ) );

                Compress( inputFile, outputFile, blockSize, mode, level, state, LZSSE8_CompressOptimalParse );

                LZSSE8_FreeOptimalParseState( state );
            }
            else
            {
                LZSSE8_FastParseState* state = LZSSE8_MakeFastParseState();

                Compress( inputFile, outputFile, blockSize, mode, level, state, CompressorFastLZSSE8 );

                LZSSE8_FreeFastParseState( state );
            }

            break;
        }

        default:

            printf( "Invalid stream type\n" );
            exit( 1 );

        }
    }

    fclose( inputFile );
    fclose( outputFile );

    return 0;
}

