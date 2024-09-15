#include "Client/AutocompleteModel.h"
#include "Parsers/Lexer.h"

#include <iostream>
#include <filesystem>
#include <fstream>
#include <vector>
#include <string>


std::vector<std::string> preprocessStringQuery(const std::string& query, AutocompleteModel& model) {
    auto lexer = DB::Lexer(query.data(), query.data() + query.size(), query.size());

    auto [preprocessed_for_tf, preprocessed_for_markov] = model.preprocessTokens(lexer);

    return preprocessed_for_tf;
}


int main() {
    std::string folder_1, folder_2;
    std::cout << "Enter folder_1 path: ";
    std::cin >> folder_1;
    std::cout << "Enter folder_2 path: ";
    std::cin >> folder_2;

    auto model = AutocompleteModel();

    std::filesystem::create_directories(folder_2);

    for (const auto& entry : std::filesystem::recursive_directory_iterator(folder_1)) {
        if (entry.is_regular_file()) {
            std::ifstream input_file(entry.path());
            if (!input_file.is_open()) {
                std::cerr << "Could not open file: " << entry.path() << std::endl;
                continue;
            }

            std::filesystem::path relative_path = std::filesystem::relative(entry.path(), folder_1);
            std::filesystem::path output_file_path = folder_2 / relative_path;

            std::filesystem::create_directories(output_file_path.parent_path());

            std::ofstream output_file(output_file_path);
            if (!output_file.is_open()) {
                std::cerr << "Could not open file: " << output_file_path << std::endl;
                continue;
            }

            std::string line;
            while (std::getline(input_file, line)) {
                std::vector<std::string> result = preprocessStringQuery(line, model);

                if (result.size() >= 2) {
                    if (result[0] == "SELECT" && result[1] == "Operator") {
                        std::cout << entry.path() << "\n";
                        std::cout << line << "\n";
                    }
                }

                if (!result.empty()) {
                    for (size_t i = 0; i < result.size(); ++i) {
                        output_file << result[i];
                        if (i != result.size() - 1) {
                            output_file << " ";
                        }
                    }
                    output_file << "\n";
                }
            }

            input_file.close();
            output_file.close();
        }
    }

    return 0;
}
