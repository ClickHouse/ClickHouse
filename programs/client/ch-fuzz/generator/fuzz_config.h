#include <fstream>
#include <filesystem>
#include <string>

#include "../third_party/json.h"
using json = nlohmann::json;

namespace chfuzz {

class FuzzConfig {
public:
	uint32_t seed = 0;
	std::filesystem::path log_path = std::filesystem::temp_directory_path() / "out.sql",
						  db_file_path = std::filesystem::temp_directory_path() / "db";

	FuzzConfig(const std::string &path) {
		std::ifstream ifs(path);
		const json jdata = json::parse(ifs);

		for (const auto& [key, value] : jdata.items()) {
			if (key == "db_file_path") {
				db_file_path = std::filesystem::path(value);
			} else if (key == "log_path") {
				log_path = std::filesystem::path(value);
			} else if (key == "seed") {
				seed = static_cast<uint32_t>(value);
			} else {
				throw std::runtime_error("Unknown option: " + key);
			}
		}
	}
};

}
