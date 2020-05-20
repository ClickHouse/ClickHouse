
#include <iostream>
#include <string>
#include <iostream>
#include <chrono>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/info_parser.hpp>
#include "dealer.h"

int main(int argc, char const *argv[])
{

    if(argc!=2){
        std::cerr << "\nUsage: " << argv[0] << " info_config_file_name\n";
        return 0;
    }
    const char *property_tree_file_name=argv[1];
    boost::property_tree::ptree all_properties;
    boost::property_tree::info_parser::read_info(property_tree_file_name, all_properties);


    bool load_buffer_file=false;
    bool save_buffer_file=false;
    std::string load_buffer_file_name=all_properties.get<std::string>("load buffers from binary file","");
    if(load_buffer_file_name.size()!=0)
        load_buffer_file=true;
    std::string save_buffer_file_name=all_properties.get<std::string>("save file name","");
    if(save_buffer_file_name.size()!=0)
        save_buffer_file=true;


    char CSV_separator=all_properties.get<char>("CSV separator", ',');
    bool print_CSV = all_properties.get<bool>("print CSV", false);
    int text_length=all_properties.get<int>("text length", 100);

    int text_stddev=all_properties.get<int>("text stddev", 15);
    int key_length=all_properties.get<int>("key length", 20);
    int key_stddev=all_properties.get<int>("key stddev", 10);
    int number_of_unique_keys=all_properties.get<int>("number of unique keys", 500);
    int number_of_unique_texts=all_properties.get<int>("number of unique texts", 500);
    
    unsigned long int maximum_number_of_records_to_permute=all_properties.get<unsigned long int>("rec_perm", 0);

    char *buffer_keys;
    char *buffer_texts;
    unsigned int *buffer_keys_length;
    unsigned int *buffer_texts_length;
    unsigned long int *buffer_keys_position;
    unsigned long int *buffer_texts_position; 
    unsigned long int *buffer_permutation_indexes; 

    //unsigned int memory_GB = all_properties.get<unsigned int>("maximum memory in GB", 0);
    unsigned int memory_MB = all_properties.get<unsigned int >("maximum memory in MB", 1);
    unsigned long int memory_used=memory_MB*1024*1024UL;
    

    unsigned long int mem_keys, mem_texts;

    auto start = std::chrono::steady_clock::now();
    
    dealer DD(memory_used,text_length,text_stddev,key_length,key_stddev, number_of_unique_keys,number_of_unique_texts, buffer_keys, buffer_keys_length, buffer_keys_position, buffer_texts, buffer_texts_length, buffer_texts_position, buffer_permutation_indexes);

    if(load_buffer_file)
        DD.read_buffers_from_file((const char*)(load_buffer_file_name.c_str()));
    else{    
        DD.build_buffers(mem_keys, mem_texts, maximum_number_of_records_to_permute);
    }


    auto end = std::chrono::steady_clock::now();
    auto diff = end - start;
    std::cerr << std::chrono::duration <double, std::milli> (diff).count()/1000.0 << " s" << std::endl;

    if(print_CSV)
        DD.csv_stdout(',');
    

    if(save_buffer_file)
        DD.write_buffers_to_file((const char*)(save_buffer_file_name.c_str()));

    free(buffer_keys);
    std::cerr << "free(buffer_keys);" << std::endl;
    free(buffer_keys_length);
    std::cerr << "free(buffer_keys_length);" << std::endl;
    free(buffer_keys_position);
    std::cerr << "free(buffer_keys_position);" << std::endl;

    free(buffer_texts);
    std::cerr << "free(buffer_texts);" << std::endl;
    free(buffer_texts_length);
    std::cerr << "free(buffer_texts_length);" << std::endl;
    free(buffer_texts_position);
    std::cerr << "free(buffer_texts_position);" << std::endl;
    free(buffer_permutation_indexes);
    std::cerr << "free(buffer_permutation_indexes);" << std::endl;

    return 0;
}