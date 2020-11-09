#include <cassert>
#include <fstream>
#include <sstream>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* Data, std::size_t Size);

int main(int argc, char* argv[]) {
  for (int i = 1; i < argc; ++i) {
    std::ifstream in(argv[i]);
    assert(in);
    in.seekg(0, std::ios_base::end);
    const auto pos = in.tellg();
    assert(pos >= 0);
    in.seekg(0, std::ios_base::beg);
    std::vector<char> buf(static_cast<std::size_t>(pos));
    in.read(buf.data(), static_cast<long>(buf.size()));
    assert(in.gcount() == pos);
    LLVMFuzzerTestOneInput(reinterpret_cast<const uint8_t*>(buf.data()),
                           buf.size());
  }
}
