#include <snappy.h>
#include <IO/HadoopSnappyReadBuffer.h>
#include <gtest/gtest.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Poco/Base64Decoder.h>
#include <Poco/Base64Encoder.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>
#include <Poco/String.h>
#include <Common/SipHash.h>
#include <Common/hex.h>
using namespace DB;
TEST(HadoopSnappyDecoder, repeatNeedMoreInput)
{
    String snappy_base64_content = "AAAl6gAAB67qSzQyMDIxLTA2LTAxAXh4eGIEACQzMTkuNzQyNDMKnjEAHDQyLjgyMTcynjEAIDI5Ni4yODQwNqIxA"
                                   "BgyNy43MjYzqpMAGDMuNTIyMzSiYgAcNjUuNDk1OTeiMQAYOTQuNTg1NaYxABg4OC40NzgyojEAHDMyMS4zOTE1os"
                                   "QAHDM0Ni4xNTI3qjEAGDEuMjA3MTWm9QAQMi41MjamYQAcMjIuNTEyNDieYQAcMzMwLjI5MTKiGgIcMzIzLjAzNDi"
                                   "iwwAcMzE1LjA1MDmiYgAcNDM1Ljc2ODaqxAAUMS45NDA5nvQACDAuMP4rAEorABwzMDMuMjAyNaYZARgwOC4xOTEy"
                                   "pugAGDQ2LjQ0MjKilQMcMjc4Ljk3MTiiMQAcMzUwLjc3NTeirAGqSwEcMzI5LjkyMzGiXAAcMzMxLjc2NzamwAMUM"
                                   "TMuNjM4pjEAGDI3NC4yMzK2MQAINDg0qrMBFDExLjgzNqbbBRgyNDkuNTI5qtsFGDUwLjE4ODmi5AGlSAgwNjWmiA"
                                   "EUMjIuNjU4pqcCBDUzYcCqdgIYMDEuMzcxNbbPBgQ5Na5TBBA0Ljc1OaIiBMGdDDM0OTGeJwQcMjg3LjIyNTmm/AM"
                                   "hVAAyopAAGDMxOC4wMjGmMAAB8AQ0OKpGAhgyMC42MTM4poMBFDg3LjEzOKoxABA5My4xNaZSARQ5NS41ODemTgVh"
                                   "OQwwODg2osIAGDMyNi45NTSmMQAcMjc3LjgxNDmqjQcMNS42MqpqA0F3DDg2MDamzAPhKwQ4OKJWARgzMDYuMTc1q"
                                   "i0EGDgwLjIwNTSihAUYMjk3LjY5NaYiBRAyOTAuM6aNBBgyMzkuMzI5pkIJwdOi7wcYMzcxLjIyNqpiBxQ0NS44Nz"
                                   "Gq9woEODAOZAoANqJ+BRgyNzYuMjExpnYCIYIMMjIyOKKnAmVrBDc0psQAEDMwOS4xqtEJGDMwNC45MzSq8wAMNC4"
                                   "0OKomCyG3DDE4MTGi/AMhJAQxMKqjBhgyNjEuNDQ4rqMGFDIuOTEwN6I5AwQzN7JMCQw2LjcwqqoMGDI2MC44NzOm"
                                   "dwIOTAkMNDgzMqLSBhQyNTkuMjGmYweBiwg3MzOmyQMYNDM3Ljg1N6ZyBq5QARQzMy43MjSqKw4UMTIuNzkxpkkFD"
                                   "mgNDDc4MzCmUAEUOTUuOTQypnoFDiQIDDI2ODmmBQMUNTEuMjc2qikEDtkJBDA1qgUDFDA3LjE1N6ZiAOGUCDMwOa"
                                   "oxABA3NC42NqqmAhA5Ni45N6rIAxwzMDcuMjkzMaL+ChQyNzUuODau/QoANOExpugBGDI0Ny4xODSm5wEYOTEuNDE"
                                   "3MZ7MChQzMzUuNjWquQQUNTMuODg1psMHDu8SCDIyOaYJDoFbCDk4M6aWDhwzNDEuNTcyMKK1AUF4ADSqCwoQMzg1"
                                   "LjSujBIB9Aw0MDUwotoJDi4PCDc0N6aHARgyMjMuODMxpgYRwmcRGDIxMi4xNjWqSgIQMDkuODmuzgMYMTkuNTg0M"
                                   "aK7CMFFADmuZQcQMDYuMzKqXwAIOS4zrl8ADu4PBDQ0qtQUGDQ3LjAzODGmFwIYMTAuOTIwMKLDAKG0DDM3MDOiYg"
                                   "CqNgcORgkEMzeuGwWqXQAhqwg2MDWmSQUYMjY0LjE2N6aZFBIgFgQyM6aiCRQwNi41NTSm9AcYMjczLjczNqqSABg"
                                   "0NS45OTIzpugPFDIxLjc3MqZ4EBwyODYuMDkyNKZAAhg0OS4yMjQzom8GDu0LCDEwNKaTBwAzDiUIADimGQkUMzM4"
                                   "Ljc2qlITADcOmBUAOaYNBhwyNzAuODA4N6qrCQw3LjAwppkYwT4IMjYzrg0GDDMuOTmq/xEQMjIuODOqRgkEMjQOX"
                                   "xKmQA0IMzAwDggVqjwREDY1LjYxsh8aCDQuOKrCBxgyNTQuNjQ2phMUISQENzmqsAwOLgsENTWqeAIQOTEuNTiuzR"
                                   "EANw55CQAwpp8GEDI2My44rgsRFDI0LjMxNqZuBhIrFgAxqswDGDI4OS4zMzCqXwQANoHyADCmbAMUMzI4LjM2pps"
                                   "DDDY1LjKBj57+Cg5PFwQ1NaoVBmFrADaqwgccMjk5LjgxMTCqdwYQMy4wODKmZwcEMzIOqBQAMaaCBRgyMjUuMTE2"
                                   "qtkJADEOLw8AMKYwBBgyMzAuMTQyprwPGDMwMi4wMjemiAEOzQ4MODA0M6YaAhA1NC4yNKYkBWEMDsELqmEAFDIuN"
                                   "jE4N6LNBxgyODMuNTM1qqUfFDk5Ljc4NKaaGQ5UEAgyNjSuqw2usgkYNDMuMDY0MZ5rAyHkCDMzOa6sHg6+CwAwpn"
                                   "YGDnseCDk1MqaoAsHYDDgzNjeiLgsYMjg1LjkzMqZ1EQ67IQgyNTmmMQBB2Qg0OTamuhMUMjcxLjkzqpMWBDMyDoo"
                                   "hADSmYgChhAg2NjimeAIQMzkxLjiqyw4IOTkuDt8bpoYBDDk0LjamaQMO4hAIOTI3qqQYFDQyLjk1M6oxAAQ4NA7G"
                                   "HaZKIg6YCwwxNzYzpiQXFDkwLjk0OKqqAhQ5Ny4yNzSmvwQANg54GKq/CA4AIQg1MzOm/wMUNTYuNzQ2phcCHDM0N"
                                   "S4wOTEyoswHDoAQCDA5M6rOGRA5MS42N6ZPGyQyNzUuNzExMTIK";
    String snappy_content;
    Poco::MemoryInputStream istr(snappy_base64_content.data(), snappy_base64_content.size());
    Poco::Base64Decoder decoder(istr);
    Poco::StreamCopier::copyToString(decoder, snappy_content);
    auto file_writer = std::make_unique<WriteBufferFromFile>("./test.snappy");
    file_writer->write(snappy_content.c_str(), snappy_content.size());
    file_writer->close();
    std::unique_ptr<ReadBuffer> in = std::make_unique<ReadBufferFromFile>("./test.snappy", 128);
    HadoopSnappyReadBuffer read_buffer(std::move(in));
    String output;
    WriteBufferFromString out(output);
    copyData(read_buffer, out);
    UInt128 hashcode = sipHash128(output.c_str(), output.size());
    String hashcode_str = getHexUIntLowercase(hashcode);
    ASSERT_EQ(hashcode_str, "593afe14f61866915cc00b8c7bd86046");
}
