#include <IO/WriteBufferAIO.h>
#include <Core/Defines.h>

#include <boost/filesystem.hpp>

#include <iostream>
#include <fstream>
#include <streambuf>
#include <cstdlib>

namespace
{

namespace fs = boost::filesystem;

void run();
void die(const std::string & msg);
void runTest(unsigned int num, const std::function<bool()> func);
std::string createTmpFile();
std::string generateString(size_t n);

bool test1();
bool test2();
bool test3();
bool test4();
bool test5();
bool test6();
bool test7();
bool test8();
bool test9();
bool test10();

void run()
{
    const std::vector<std::function<bool()> > tests =
    {
        test1,
        test2,
        test3,
        test4,
        test5,
        test6,
        test7,
        test8,
        test9,
        test10
    };

    unsigned int num = 0;
    for (const auto & test : tests)
    {
        ++num;
        runTest(num, test);
    }
}

void die(const std::string & msg)
{
    std::cout << msg;
    ::exit(EXIT_FAILURE);
}

void runTest(unsigned int num, const std::function<bool()> func)
{
    bool ok;

    try
    {
        ok = func();
    }
    catch (const DB::Exception & ex)
    {
        ok = false;
        std::cout << "Caught exception " << ex.displayText() << "\n";
    }
    catch (const std::exception & ex)
    {
        ok = false;
        std::cout << "Caught exception " << ex.what() << "\n";
    }

    if (ok)
        std::cout << "Test " << num << " passed\n";
    else
        std::cout << "Test " << num << " failed\n";
}

std::string createTmpFile()
{
    char pattern[] = "/tmp/fileXXXXXX";
    char * dir = ::mkdtemp(pattern);
    if (dir == nullptr)
        die("Could not create directory");

    return std::string(dir) + "/foo";
}

std::string generateString(size_t n)
{
    static const std::string symbols = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    std::string buf;
    buf.reserve(n);

    for (size_t i = 0; i < n; ++i)
        buf += symbols[i % symbols.length()];

    return buf;
}

bool test1()
{
    std::string filename = createTmpFile();

    size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;

    std::string buf = generateString(n);

    {
        DB::WriteBufferAIO out(filename, 3 * DEFAULT_AIO_FILE_BLOCK_SIZE);

        if (out.getFileName() != filename)
            return false;
        if (out.getFD() == -1)
            return false;

        out.write(&buf[0], buf.length());
    }

    std::ifstream in(filename.c_str());
    if (!in.is_open())
        die("Could not open file");

    std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

    in.close();
    fs::remove_all(fs::path(filename).parent_path().string());

    return (received == buf);
}

bool test2()
{
    std::string filename = createTmpFile();

    size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;

    std::string buf = generateString(n);

    {
        DB::WriteBufferAIO out(filename, 3 * DEFAULT_AIO_FILE_BLOCK_SIZE);

        if (out.getFileName() != filename)
            return false;
        if (out.getFD() == -1)
            return false;

        out.write(&buf[0], buf.length() / 2);
        out.seek(DEFAULT_AIO_FILE_BLOCK_SIZE, SEEK_CUR);
        out.write(&buf[buf.length() / 2], buf.length() / 2);
    }

    std::ifstream in(filename.c_str());
    if (!in.is_open())
        die("Could not open file");

    std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

    in.close();
    fs::remove_all(fs::path(filename).parent_path().string());

    if (received.substr(0, buf.length() / 2) != buf.substr(0, buf.length() / 2))
        return false;
    if (received.substr(buf.length() / 2, DEFAULT_AIO_FILE_BLOCK_SIZE) != std::string(DEFAULT_AIO_FILE_BLOCK_SIZE, '\0'))
        return false;
    if (received.substr(buf.length() / 2 + DEFAULT_AIO_FILE_BLOCK_SIZE) != buf.substr(buf.length() / 2))
        return false;

    return true;
}

bool test3()
{
    std::string filename = createTmpFile();

    size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;

    std::string buf = generateString(n);

    {
        DB::WriteBufferAIO out(filename, 3 * DEFAULT_AIO_FILE_BLOCK_SIZE);

        if (out.getFileName() != filename)
            return false;
        if (out.getFD() == -1)
            return false;

        out.write(&buf[0], buf.length());

        off_t pos1 = out.getPositionInFile();

        out.truncate(buf.length() / 2);

        off_t pos2 = out.getPositionInFile();

        if (pos1 != pos2)
            return false;
    }

    std::ifstream in(filename.c_str());
    if (!in.is_open())
        die("Could not open file");

    std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

    in.close();
    fs::remove_all(fs::path(filename).parent_path().string());

    return (received == buf.substr(0, buf.length() / 2));
}

bool test4()
{
    std::string filename = createTmpFile();

    size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;

    std::string buf = generateString(n);

    {
        DB::WriteBufferAIO out(filename, 3 * DEFAULT_AIO_FILE_BLOCK_SIZE);

        if (out.getFileName() != filename)
            return false;
        if (out.getFD() == -1)
            return false;

        out.write(&buf[0], buf.length());

        off_t pos1 = out.getPositionInFile();

        out.truncate(3 * buf.length() / 2);

        off_t pos2 = out.getPositionInFile();

        if (pos1 != pos2)
            return false;
    }

    std::ifstream in(filename.c_str());
    if (!in.is_open())
        die("Could not open file");

    std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

    in.close();
    fs::remove_all(fs::path(filename).parent_path().string());

    if (received.substr(0, buf.length()) != buf)
        return false;

    if (received.substr(buf.length()) != std::string(buf.length() / 2, '\0'))
        return false;

    return true;
}

bool test5()
{
    std::string filename = createTmpFile();

    size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;

    std::string buf = generateString(n);

    {
        DB::WriteBufferAIO out(filename, 3 * DEFAULT_AIO_FILE_BLOCK_SIZE);

        if (out.getFileName() != filename)
            return false;
        if (out.getFD() == -1)
            return false;

        out.seek(1, SEEK_SET);
        out.write(&buf[0], buf.length());
    }

    std::ifstream in(filename.c_str());
    if (!in.is_open())
        die("Could not open file");

    std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

    in.close();
    fs::remove_all(fs::path(filename).parent_path().string());

    return received.substr(1) == buf;
}

bool test6()
{
    std::string filename = createTmpFile();

    size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;

    std::string buf = generateString(n);

    std::string buf2 = "1111111111";

    {
        DB::WriteBufferAIO out(filename, 3 * DEFAULT_AIO_FILE_BLOCK_SIZE);

        if (out.getFileName() != filename)
            return false;
        if (out.getFD() == -1)
            return false;

        out.seek(3, SEEK_SET);
        out.write(&buf[0], buf.length());
        out.seek(-2 * DEFAULT_AIO_FILE_BLOCK_SIZE, SEEK_CUR);
        out.write(&buf2[0], buf2.length());
    }

    std::ifstream in(filename.c_str());
    if (!in.is_open())
        die("Could not open file");

    std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

    in.close();
    fs::remove_all(fs::path(filename).parent_path().string());

    if (received.substr(3, 8 * DEFAULT_AIO_FILE_BLOCK_SIZE) != buf.substr(0, 8 * DEFAULT_AIO_FILE_BLOCK_SIZE))
        return false;

    if (received.substr(3 + 8 * DEFAULT_AIO_FILE_BLOCK_SIZE, 10) != buf2)
        return false;

    if (received.substr(13 + 8 * DEFAULT_AIO_FILE_BLOCK_SIZE) != buf.substr(10 + 8 * DEFAULT_AIO_FILE_BLOCK_SIZE))
        return false;

    return true;
}

bool test7()
{
    std::string filename = createTmpFile();

    std::string buf2 = "11111111112222222222";

    {
        DB::WriteBufferAIO out(filename, DEFAULT_AIO_FILE_BLOCK_SIZE);

        if (out.getFileName() != filename)
            return false;
        if (out.getFD() == -1)
            return false;

        out.seek(DEFAULT_AIO_FILE_BLOCK_SIZE - (buf2.length() / 2), SEEK_SET);
        out.write(&buf2[0], buf2.length());
    }

    std::ifstream in(filename.c_str());
    if (!in.is_open())
        die("Could not open file");

    std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

    if (received.length() != 4106)
        return false;
    if (received.substr(0, 4086) != std::string(4086, '\0'))
        return false;
    if (received.substr(4086, 20) != buf2)
        return false;

    in.close();
    fs::remove_all(fs::path(filename).parent_path().string());

    return true;
}

bool test8()
{
    std::string filename = createTmpFile();

    std::string buf2 = "11111111112222222222";

    {
        DB::WriteBufferAIO out(filename, 2 * DEFAULT_AIO_FILE_BLOCK_SIZE);

        if (out.getFileName() != filename)
            return false;
        if (out.getFD() == -1)
            return false;

        out.seek(2 * DEFAULT_AIO_FILE_BLOCK_SIZE - (buf2.length() / 2), SEEK_SET);
        out.write(&buf2[0], buf2.length());
    }

    std::ifstream in(filename.c_str());
    if (!in.is_open())
        die("Could not open file");

    std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

    if (received.length() != 8202)
        return false;
    if (received.substr(0, 8182) != std::string(8182, '\0'))
        return false;
    if (received.substr(8182, 20) != buf2)
        return false;

    in.close();
    fs::remove_all(fs::path(filename).parent_path().string());

    return true;
}

bool test9()
{
    std::string filename = createTmpFile();

    size_t n = 3 * DEFAULT_AIO_FILE_BLOCK_SIZE;

    std::string buf = generateString(n);

    std::string buf2(DEFAULT_AIO_FILE_BLOCK_SIZE + 10, '1');

    {
        DB::WriteBufferAIO out(filename, 2 * DEFAULT_AIO_FILE_BLOCK_SIZE);

        if (out.getFileName() != filename)
            return false;
        if (out.getFD() == -1)
            return false;

        out.seek(3, SEEK_SET);
        out.write(&buf[0], buf.length());
        out.seek(-DEFAULT_AIO_FILE_BLOCK_SIZE, SEEK_CUR);
        out.write(&buf2[0], buf2.length());
    }

    std::ifstream in(filename.c_str());
    if (!in.is_open())
        die("Could not open file");

    std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

    in.close();
    fs::remove_all(fs::path(filename).parent_path().string());

    if (received.substr(3, 2 * DEFAULT_AIO_FILE_BLOCK_SIZE) != buf.substr(0, 2 * DEFAULT_AIO_FILE_BLOCK_SIZE))
        return false;

    if (received.substr(3 + 2 * DEFAULT_AIO_FILE_BLOCK_SIZE, DEFAULT_AIO_FILE_BLOCK_SIZE + 10) != buf2)
        return false;

    return true;
}

bool test10()
{
    std::string filename = createTmpFile();

    size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE + 3;

    std::string buf = generateString(n);

    {
        DB::WriteBufferAIO out(filename, 3 * DEFAULT_AIO_FILE_BLOCK_SIZE);

        if (out.getFileName() != filename)
            return false;
        if (out.getFD() == -1)
            return false;

        out.write(&buf[0], buf.length());
    }

    std::ifstream in(filename.c_str());
    if (!in.is_open())
        die("Could not open file");

    std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

    in.close();
    fs::remove_all(fs::path(filename).parent_path().string());

    return (received == buf);
}

}

int main()
{
    run();
    return 0;
}
