#pragma once

#include <iostream>
#include <filesystem>
#include <string>
#include <vector>
#include <boost/program_options.hpp>
#include <Core/Types.h>

int executeScript(const std::string & command, bool throw_on_error = false);
bool ask(std::string question);
bool filesEqual(std::string path1, std::string path2);

void installBinaries(boost::program_options::variables_map & options, std::vector<std::string> & symlinks);
void createUsers(boost::program_options::variables_map & options);
bool setupConfigsAndDirectories(boost::program_options::variables_map & options);
void createPasswordAndFinish(boost::program_options::variables_map & options, bool has_password_for_default_user);

int start(const std::string & user, const std::filesystem::path & executable, const std::filesystem::path & config, const std::filesystem::path & pid_file);
UInt64 isRunning(const std::filesystem::path & pid_file, const std::string & program_name);
int stop(const std::filesystem::path & pid_file, bool force, const std::string & program_name);
