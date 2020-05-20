#pragma once

#include <stdexcept>
#include <iostream>
#include <vector>
#include <random>
#include <string>
#include <functional> //for std::function
#include <algorithm>  //for std::generate_n
#include <cstring>
#include <cmath>
#include <fstream>
#include <cstdlib>

//struct to hold the character set
typedef std::vector<char> char_array;

//this structure holds one record of (key)|(text)
struct record{
    char *value=nullptr;
    unsigned int current_ID;
    unsigned int value_length=0;

    //using deafualt MOVE constructor
    record(record&& tmp):
    value(tmp.value),
    current_ID(tmp.current_ID),
    value_length(tmp.value_length),
    active(tmp.active),
    updated(tmp.updated),
    initialized(tmp.initialized)
    {
       // std::cout << "default constructor called" << std::endl;
        tmp.value=nullptr;

    };

    //make a true copy by the copy constructor
/*    record(const record& other){
        std::cout << "Copy constructor ";
        key = new char[key_length];
        text = new char[text_length];
        
        key_length=other.key_length;
        text_length=other.text_length;

        memcpy(key, other.key,  key_length);
        memcpy(text, other.text,  text_length);
        active=other.active;
        updated=other.updated;
        initialized=other.initialized;

    }
*/  
    record& operator= (const record &other){
        //std::cout << "= operator ";
        if (this != &other){
            value_length=other.value_length;
            if(value!=nullptr)
                delete [] value;
            value=new char[value_length];                 

            memcpy(value, other.value, value_length);
            //active=other.active;
            updated=true;
            //initialized=other.initialized;

        }
        return *this;
    }

    ~record(){
        //ussue here with the copy constructor!!!
        //everyone suggest to use std::string, but i can't, cause there's too much memory used.
        //I assume that i must use MOVE constructor: record(record&& tmp)
       // std::cout << "destructor called" << std::endl;
        delete [] value;
       //std::cout << "distructor";

    };
    
    record(std::string value_1, unsigned int current_ID_1){
        set_record(value_1);
        current_ID=current_ID_1;
        updated=false;
        initialized=true;
    };


    void set_record(std::string value_1){
        if(value!=nullptr)
            delete [] value;

        value_length=value_1.size();
        //value_length+=1; // '\0' included
        value=new char[value_length+1];
        
        value_1.copy(value,value_length,0);
        value[value_length]='\0';
        value_length+=1;

    }


    void set_value(const record& other){
        if (this != &other){
            value_length=other.value_length;
            if(value!=nullptr)
                delete [] value;
            value=new char[value_length];
            memcpy(value, other.value, value_length);
            //value[value_length]='\0';
            updated=true;
        }

    }

    unsigned int record_size(){
        return (value_length)*sizeof(char);
    }

    void activate(){
        active=true;
    }    

    void deactivate(){
        active=false;
    }
    bool status(){
        return active;
    }

    void print(){
        std::cout << value;// << std::endl;
    }
    void print_debug(){
        std::cout << value << "(" << value_length*sizeof(char) << ")"  << std::endl;
    }

    int full_size(){
        return (value_length)*sizeof(char)+sizeof(record);
    }



private:
    bool active=false;
    bool updated=false;
    bool initialized=false;

};




class dealer{
private:
    bool all_set=false;
    bool from_file=false;
    int text_length={};
    int text_stddev={};
    int key_length={};
    int key_stddev={};  
    unsigned long int maximum_mem={};
    unsigned long int mem_keys;
    unsigned long int mem_texts;
    unsigned long int unique_mem_keys;
    unsigned long int unique_mem_texts;
    float number_of_unique_keys={};
    float number_of_unique_texts={};
    int size_of_unique_keys={};
    int size_of_unique_texts={};
    
    unsigned long int dumped_number_of_records=0;

    int *indexes_of_unique_keys;
    int *indexes_of_unique_texts;


    std::vector<record> key_buffer;
    std::vector<record> text_buffer;

    std::uniform_int_distribution<int> *uniform_int_distribution;
    std::default_random_engine *rng;
    std::normal_distribution<double> *normal_distribution_text;
    std::normal_distribution<double> *normal_distribution_key;
    //std::uniform_int_distribution<int> *uniform_int_distribution_generator_buffer;



    char_array charset()
    {
        //Change this to suit
        return char_array( 
        {'0','1','2','3','4',
        '5','6','7','8','9',
        'A','B','C','D','E','F',
        'G','H','I','J','K',
        'L','M','N','O','P',
        'Q','R','S','T','U',
        'V','W','X','Y','Z',
        'a','b','c','d','e','f',
        'g','h','i','j','k',
        'l','m','n','o','p',
        'q','r','s','t','u',
        'v','w','x','y','z'
        });
    };   
    const char_array ch_set = charset(); 

    std::string random_string( size_t length, std::function<char(void)> rand_char )
    {
        std::string str(length,0);
        std::generate_n( str.begin(), length, rand_char);
        return str;
    }

public:
    
    unsigned int buffer_size;

    char *&buffer_keys;
    char *&buffer_texts;
    unsigned int *&buffer_keys_length;
    unsigned int *&buffer_texts_length;
    unsigned long int *&buffer_keys_position;
    unsigned long int *&buffer_texts_position;    
    unsigned long int *&buffer_permutation_indexes; 


    unsigned int get_mem_keys()const { return mem_keys; }
    unsigned int get_mem_texts()const { return mem_texts; }

    dealer(char *&buffer_keys_, unsigned int *&buffer_keys_length_, unsigned long int *&buffer_keys_position_, char *&buffer_texts_, unsigned int *&buffer_texts_length_, unsigned long int *&buffer_texts_position_, unsigned long int *&buffer_permutation_indexes_) : 
        buffer_keys(buffer_keys_),
        buffer_keys_length(buffer_keys_length_),
        buffer_keys_position(buffer_keys_position_),

        buffer_texts(buffer_texts_), 
        buffer_texts_length(buffer_texts_length_),
        buffer_texts_position(buffer_texts_position_),

        buffer_permutation_indexes(buffer_permutation_indexes_)
    {
    }
    dealer(unsigned long int maximum_mem_, unsigned int text_length_, unsigned int text_stddev_, unsigned int key_length_, unsigned int key_stddev_, int number_of_unique_keys_, int number_of_unique_texts_, char *&buffer_keys_, unsigned int *&buffer_keys_length_, unsigned long int *&buffer_keys_position_, char *&buffer_texts_, unsigned int *&buffer_texts_length_, unsigned long int *&buffer_texts_position_, unsigned long int *&buffer_permutation_indexes_):
        
        text_length(text_length_),
        maximum_mem(maximum_mem_),// DEBUG!!!*1024*1024*1024),
        text_stddev(text_stddev_),
        key_length(key_length_),
        key_stddev(key_stddev_),
        number_of_unique_keys(number_of_unique_keys_),
        number_of_unique_texts(number_of_unique_texts_),

        buffer_keys(buffer_keys_),
        buffer_keys_length(buffer_keys_length_),
        buffer_keys_position(buffer_keys_position_),

        buffer_texts(buffer_texts_), 
        buffer_texts_length(buffer_texts_length_),
        buffer_texts_position(buffer_texts_position_),

        buffer_permutation_indexes(buffer_permutation_indexes_)
    {   
        

        mem_keys=(unsigned long int)maximum_mem*((double)key_length/(key_length+text_length));
        mem_texts=(unsigned long int)maximum_mem*((double)text_length/(key_length+text_length));

        unique_mem_keys=(unsigned long int)number_of_unique_keys*key_length*sizeof(char);
        unique_mem_texts=(unsigned long int)number_of_unique_texts*text_length*sizeof(char);


        size_of_unique_keys=(unsigned int)number_of_unique_keys;
        size_of_unique_texts=(unsigned int)number_of_unique_texts;

        /*    
        std::cerr << "Using memory:" << maximum_mem << std::endl;
        std::cerr << "Statistical unique elements: keys="  << size_of_unique_keys << " texts=" << size_of_unique_texts << std::endl;
        std::cerr << "Statistical total sizes: keys="  << mem_keys << " texts=" << mem_texts << std::endl;
        */

        std::random_device rd("/dev/urandom");
        rng=new std::default_random_engine(rd());
        uniform_int_distribution=new std::uniform_int_distribution<int>(0, ch_set.size()-1);
        normal_distribution_text=new std::normal_distribution<double>{(double)text_length,(double)text_stddev};
        normal_distribution_key=new std::normal_distribution<double>{(double)key_length,(double)key_stddev};

        
        indexes_of_unique_keys=new int[size_of_unique_keys];
        indexes_of_unique_texts=new int[size_of_unique_texts];
       

    }
    dealer() = default;
    
    dealer(const char *filename, char *&buffer_keys_, unsigned int *&buffer_keys_length_, unsigned long int *&buffer_keys_position_, char *&buffer_texts_, unsigned int *&buffer_texts_length_, unsigned long int *&buffer_texts_position_, unsigned long int *&buffer_permutation_indexes_):
        buffer_keys(buffer_keys_),
        buffer_keys_length(buffer_keys_length_),
        buffer_keys_position(buffer_keys_position_),

        buffer_texts(buffer_texts_), 
        buffer_texts_length(buffer_texts_length_),
        buffer_texts_position(buffer_texts_position_),

        buffer_permutation_indexes(buffer_permutation_indexes_)
    {

        read_buffers_from_file(filename);

    }


    ~dealer(){

        if(!from_file){
            delete rng;
            delete uniform_int_distribution;
            delete normal_distribution_text;
            delete normal_distribution_key;
            delete [] indexes_of_unique_keys;
            delete [] indexes_of_unique_texts;
        }

    }

    void write_buffers_to_file(const char *filename){
        //std::ifstream infile ("test.txt",std::ifstream::binary);
        std::ofstream outfile(filename,std::ofstream::binary);
        if (!outfile) throw std::runtime_error("dealer::write_buffers_to_file: error while opening file as write" + std::string(filename));

        outfile.write(reinterpret_cast<const char *>(&dumped_number_of_records),sizeof(unsigned long int));
       
        //dumping keys
        outfile.write(reinterpret_cast<const char *>(&mem_keys),sizeof(unsigned long int));
        outfile.write(reinterpret_cast<const char *>(buffer_keys_length),dumped_number_of_records*sizeof(unsigned int));
        outfile.write(reinterpret_cast<const char *>(buffer_keys_position),dumped_number_of_records*sizeof(unsigned long int));
        outfile.write(buffer_keys, mem_keys);
        //dumping texts
        outfile.write(reinterpret_cast<const char *>(&mem_texts),sizeof(unsigned long int));
        outfile.write(reinterpret_cast<const char *>(buffer_texts_length),dumped_number_of_records*sizeof(unsigned int));
        outfile.write(reinterpret_cast<const char *>(buffer_texts_position),dumped_number_of_records*sizeof(unsigned long int));        
        outfile.write(buffer_texts, mem_texts);
        

        unsigned long int buffer_permutation_size=buffer_permutation_indexes[0];
        outfile.write(reinterpret_cast<const char *>(buffer_permutation_indexes), (2*buffer_permutation_size+1)*sizeof(unsigned long int)); 



        outfile.close();
 
    }

    void read_buffers_from_file(const char *filename){
        if(!all_set){
            from_file=true;
            std::ifstream infile(filename,std::ifstream::binary);
            if (!infile) throw std::runtime_error("dealer::read_buffers_from_file: error while opening file " + std::string(filename));
            

            infile.read(reinterpret_cast<char *>(&dumped_number_of_records),sizeof(unsigned long int));

            buffer_keys_position=(unsigned long int*)malloc(dumped_number_of_records*sizeof(unsigned long int));
            buffer_texts_position=(unsigned long int*)malloc(dumped_number_of_records*sizeof(unsigned long int));
            buffer_keys_length=(unsigned int*)malloc(dumped_number_of_records*sizeof(unsigned int));
            buffer_texts_length=(unsigned int*)malloc(dumped_number_of_records*sizeof(unsigned int));
            
            infile.read(reinterpret_cast<char *>(&mem_keys),sizeof(unsigned long int));
            buffer_keys=(char*)malloc(mem_keys*sizeof(char));

            infile.read(reinterpret_cast<char *>(buffer_keys_length),dumped_number_of_records*sizeof(unsigned int));
            infile.read(reinterpret_cast<char *>(buffer_keys_position),dumped_number_of_records*sizeof(unsigned long int));            
            infile.read(buffer_keys,mem_keys);
            infile.read(reinterpret_cast<char *>(&mem_texts),sizeof(unsigned long int));
            buffer_texts=(char*)malloc(mem_texts*sizeof(char));
        
            infile.read(reinterpret_cast<char *>(buffer_texts_length),dumped_number_of_records*sizeof(unsigned int));
            infile.read(reinterpret_cast<char *>(buffer_texts_position),dumped_number_of_records*sizeof(unsigned long int));            
            infile.read(buffer_texts,mem_texts);
             
            unsigned long int buffer_permutation_size;
            infile.read(reinterpret_cast<char *>(&buffer_permutation_size),sizeof(unsigned long int));
            
            buffer_permutation_indexes=(unsigned long int*)malloc((2*buffer_permutation_size+1)*sizeof(unsigned long int));   

            buffer_permutation_indexes[0]=buffer_permutation_size;
            infile.read(reinterpret_cast<char *>(&buffer_permutation_indexes[1]),2*buffer_permutation_size*sizeof(unsigned long int));

            infile.close();
            maximum_mem=mem_keys+mem_texts;

            std::cerr << "Total number of records=" << dumped_number_of_records << ", using memory:" << maximum_mem << " memory used per buffer: keys=" << mem_keys << " texts=" <<mem_texts << std::endl;
            all_set=true;
        }
        else{
            std::cerr << "\nClass countains set buffers, can't read file! \n";
        }
    }



    void build_buffers(unsigned long int &mem_keys_, unsigned long int &mem_texts_, unsigned long int number_of_records_to_permute){
        
        test_print();
        
        if(number_of_records_to_permute>0)
            dispence_big_buffers(number_of_records_to_permute);
        else{
            buffer_permutation_indexes=(unsigned long int*)malloc((1)*sizeof(unsigned long int));
            buffer_permutation_indexes[0]=0;
        }

        mem_keys_=mem_keys;
        mem_texts_=mem_texts;
        

    }


    void generate_unique_data(){
        auto randchar = [ this ](){
                return ch_set[ (*uniform_int_distribution)((*rng)) ];
        };

        key_buffer.clear();
        key_buffer.reserve(size_of_unique_keys);

        text_buffer.clear();
        text_buffer.reserve(size_of_unique_texts);


        for(int j=0;j<size_of_unique_keys;j++){
            int length_key = (int) (*normal_distribution_key)((*rng));
                //TODO alignment hack!!
                //length_key = (length_key/8)*8-1;
            while(length_key<=0) {
                length_key = (int) (*normal_distribution_key)((*rng));
                    //TODO alignment hack!!
                    //length_key = (length_key/8)*8-1;
            }


            std::string key=random_string(length_key,randchar);
            key_buffer.push_back(record(key, j));  
        }
        for(int j=0;j<size_of_unique_texts;j++){
            int length_text  = (int) (*normal_distribution_text)((*rng));
            while(length_text<=0){
                length_text  = (int) (*normal_distribution_text)((*rng));
            }    
            std::string text=random_string(length_text,randchar);
            text_buffer.push_back(record(text, j));                 

        }

    }


    void generate_unique_random_list_keys()
    {

        std::random_device rd1("/dev/urandom");
        std::mt19937 rand_keys(rd1());

        for (int i = 0; i < size_of_unique_keys; ++i){
            indexes_of_unique_keys[i]=i;
            
        }

        for(int j=size_of_unique_keys-1;j>=0;j--){
            std::uniform_int_distribution<int> distribution(0,j);
            int index=distribution(rand_keys);
            int temp_j_index=indexes_of_unique_keys[j];
            indexes_of_unique_keys[j]=indexes_of_unique_keys[index];
            indexes_of_unique_keys[index]=temp_j_index;
        }

    }

     void generate_unique_random_list_texts()
    {

        std::random_device rd2("/dev/urandom");
        std::mt19937 rand_texts(rd2());

        for (int i = 0; i < size_of_unique_texts; ++i){
            indexes_of_unique_texts[i]=i;
        }

        for(int j=size_of_unique_texts-1;j>=0;j--){
            std::uniform_int_distribution<int> distribution(0,j);
            int index=distribution(rand_texts);
            int temp_j_index=indexes_of_unique_texts[j];
            indexes_of_unique_texts[j]=indexes_of_unique_texts[index];
            indexes_of_unique_texts[index]=temp_j_index;
            
        }

    }


    unsigned int get_buffer_keys_used_mem(){
        unsigned int key_size=0;
        for(int j=0;j<size_of_unique_keys;j++){
            key_size+=key_buffer[j].record_size();

        }
        return key_size;
    }
    unsigned int get_buffer_texts_used_mem(){
        unsigned int text_size=0;
        for(int j=0;j<size_of_unique_texts;j++){
            text_size+=text_buffer[j].record_size();

        }
        return text_size;
    }


 
    void fill_buffer(char *&buffer_keys, unsigned int *&buffer_keys_length, unsigned long int *&buffer_keys_position, char *&buffer_texts, unsigned int *&buffer_texts_length, unsigned long int *&buffer_texts_position)
    {


 
        unsigned long int mem_keys_old=mem_keys;
        unsigned long int mem_texts_old=mem_texts;

        unsigned long int keys_bytes_dumped=0;
        unsigned long int texts_bytes_dumped=0;
        unsigned long int buffer_keys_used_mem=get_buffer_keys_used_mem();
        unsigned long int buffer_texts_used_mem=get_buffer_texts_used_mem();
        unsigned long int total_whole_memory_to_dump=0;
        unsigned long int delta_mem_keys=unique_mem_keys;
        unsigned long int delta_mem_texts=unique_mem_texts;
        unsigned long int buffer_commulative_memory=0;
        char *initial_buffer_keys=buffer_keys;
        char *initial_buffer_texts=buffer_texts;
        char *current_buffer_pointer_keys=buffer_keys;
        char *current_buffer_pointer_texts=buffer_texts;
        bool break_flag=false;
        while(!break_flag){

            if(dumped_number_of_records%(size_of_unique_keys)==0)
                generate_unique_random_list_keys();
            if(dumped_number_of_records%(size_of_unique_texts)==0)
                generate_unique_random_list_texts();


            unsigned int local_index_keys=dumped_number_of_records%(size_of_unique_keys);
            unsigned int key_size=key_buffer[indexes_of_unique_keys[local_index_keys]].record_size();
            
            unsigned int local_index_texts=dumped_number_of_records%(size_of_unique_texts);
            unsigned int text_size=text_buffer[indexes_of_unique_texts[local_index_texts]].record_size();
           
            if((keys_bytes_dumped+key_size)>mem_keys)
                break_flag=true;

            if((texts_bytes_dumped+text_size)>mem_texts)
                break_flag=true;
                           
            if(break_flag){
                mem_keys=keys_bytes_dumped+key_size;
                buffer_keys=(char*)realloc(initial_buffer_keys,mem_keys*sizeof(char));
                mem_texts=texts_bytes_dumped+text_size;
                buffer_texts=(char*)realloc(initial_buffer_texts,mem_texts*sizeof(char));
              
                current_buffer_pointer_keys=&(buffer_keys[keys_bytes_dumped]);
                current_buffer_pointer_texts=&(buffer_texts[texts_bytes_dumped]);
            }
            

            memcpy(current_buffer_pointer_keys, key_buffer[indexes_of_unique_keys[local_index_keys]].value, key_size);
            buffer_keys_length[dumped_number_of_records]=key_size;
            buffer_keys_position[dumped_number_of_records]=keys_bytes_dumped;
            keys_bytes_dumped+=key_size;
            current_buffer_pointer_keys=&(buffer_keys[keys_bytes_dumped]);

            memcpy(current_buffer_pointer_texts, text_buffer[indexes_of_unique_texts[local_index_texts]].value, text_size);
            buffer_texts_length[dumped_number_of_records]=text_size;
            buffer_texts_position[dumped_number_of_records]=texts_bytes_dumped;
            texts_bytes_dumped+=text_size;
            current_buffer_pointer_texts=&(buffer_texts[texts_bytes_dumped]);           
            
            total_whole_memory_to_dump+=keys_bytes_dumped+texts_bytes_dumped;
            
            dumped_number_of_records++;
        }

        buffer_keys_position=(unsigned long int*)realloc(buffer_keys_position,dumped_number_of_records*sizeof(unsigned long int));
        buffer_texts_position=(unsigned long int*)realloc(buffer_texts_position,dumped_number_of_records*sizeof(unsigned long int));
        buffer_keys_length=(unsigned int*)realloc(buffer_keys_length,dumped_number_of_records*sizeof(unsigned int));
        buffer_texts_length=(unsigned int*)realloc(buffer_texts_length,dumped_number_of_records*sizeof(unsigned int));

        std::cerr << "Memory Physically used: keys=" << mem_keys_old << "->" << mem_keys << ", texts=" << mem_texts_old  << "->" << mem_texts << ", total number of records=" <<  dumped_number_of_records << std::endl;

    }


    void test_print(){


        //create_new_buffer();
 
        buffer_keys=(char*)malloc(mem_keys*sizeof(char));
        buffer_texts=(char*)malloc(mem_texts*sizeof(char));

        for(int j=0;j<mem_keys;j++){
            buffer_keys[j]='?';
        }
        for(int j=0;j<mem_texts;j++){
            buffer_texts[j]='!';
        }
        generate_unique_data();
        

        unsigned int key_elements=(unsigned int)(mem_keys/((double)get_buffer_keys_used_mem())*key_buffer.size());
        unsigned int text_elements=(unsigned int)(mem_texts/((double)get_buffer_texts_used_mem())*text_buffer.size());
        unsigned int max_elements=std::max(key_elements,text_elements);

        buffer_keys_position=(unsigned long int*)malloc(max_elements*sizeof(unsigned long int));
        buffer_texts_position=(unsigned long int*)malloc(max_elements*sizeof(unsigned long int));
        buffer_keys_length=(unsigned int*)malloc(max_elements*sizeof(unsigned int));
        buffer_texts_length=(unsigned int*)malloc(max_elements*sizeof(unsigned int));


        fill_buffer(buffer_keys, buffer_keys_length, buffer_keys_position, buffer_texts, buffer_texts_length, buffer_texts_position);
        

        all_set=true;
          

        //std::for_each(buffer.begin(), buffer.end(),[this](record &record){record.print();});
        

    }


    void dispence_big_buffers(unsigned long int maximum_number_of_records){
        std::random_device rd1("/dev/urandom");
        std::mt19937 rand_seed1(rd1());
        std::random_device rd2("/dev/urandom");
        std::mt19937 rand_seed2(rd2());
        
        //std::uniform_int_distribution<unsigned short int> rand_number_of_sections=std::uniform_int_distribution<unsigned short int>(1, 200);
        std::uniform_int_distribution<unsigned long int> rand_sections=std::uniform_int_distribution<unsigned long int>(1, dumped_number_of_records);

        //unsigned short int N = rand_number_of_sections(rand_seed1);
    
        std::vector<unsigned long int>dispence_buffer;

        unsigned long int total_records=0;
        //dispence_buffer.push_back(0);
        unsigned int iterations=0;
        while(total_records<maximum_number_of_records){
            unsigned long int start=rand_sections(rand_seed2);
            unsigned long int end=rand_sections(rand_seed2);
            if(start>end) std::swap(start,end);
            if((total_records+(end-start))>=maximum_number_of_records)
                end=start+(maximum_number_of_records-total_records);
            dispence_buffer.push_back(start);
            dispence_buffer.push_back(end);
            total_records+=(end-start);
            iterations++;
        }
        //dispence_buffer.push_back( dumped_number_of_records-1);
        
        //std::sort(dispence_buffer.begin(), dispence_buffer.end());
        
        //std::vector<unsigned long int>::iterator it;
        //for (it=dispence_buffer.begin(); it!=dispence_buffer.end(); ++it)
        //    std::cout << " " << *it;
        //std::cout << std::endl << "total records permuted=" << total_records << std::endl;

        int total_elements_in_permutations=dispence_buffer.size();
        buffer_permutation_indexes=(unsigned long int*)malloc((total_elements_in_permutations+1)*sizeof(unsigned long int));
        buffer_permutation_indexes[0]=iterations;
        std::copy(dispence_buffer.begin(), dispence_buffer.end(), &buffer_permutation_indexes[1]);

        for(int j=0;j<2*iterations+1;j++){
            std::cout << " " << buffer_permutation_indexes[j];
        }
        std::cout << std::endl;

    }


    void keys_stdout(){
        if(all_set)
            for(int j=0;j<mem_keys;j++)
                std::cout << buffer_keys[j];
        else
            std::cerr << "\n Build buffers! \n";
    }   
    void texts_stdout(){
        if(all_set)
            for(int j=0;j<mem_texts;j++)
                std::cout << buffer_texts[j];
        else
            std::cerr << "\n Build buffers! \n";
    }

    void csv_stdout(const char separator)
    {
        if(buffer_permutation_indexes[0]==0)
            csv_stdout_null_terminated(separator);
        else
            csv_stdout_permutations(separator);
        
    }

    void csv_stdout_null_terminated(const char separator){
        char* local_buffer_keys=buffer_keys;
        char* local_buffer_texts=buffer_texts;
        char* local_buffer_keys_previous=buffer_keys;
        char* local_buffer_texts_previous=buffer_texts;        
        unsigned long int total_mem_read=0;
        unsigned long int total_mem_read_text=0;
        if(all_set){
            local_buffer_keys=strchr(buffer_keys, '\0' );
            local_buffer_texts=strchr(buffer_texts, '\0' );

            while(total_mem_read<mem_keys){
                unsigned long int length_key=local_buffer_keys-local_buffer_keys_previous;
                unsigned long int length_text=local_buffer_texts-local_buffer_texts_previous;            
                total_mem_read_text+=length_text+1;
                total_mem_read+=length_key+1;
                
                
                std::cout << "1980" << "-" << "03" << "-" << "16" << separator;
                std::cout << "\"";
                for(unsigned long int j=0;j<length_key;j++)
                    std::cout << local_buffer_keys_previous[j];
                
                std::cout << "\"" << separator << "\"";
                for(unsigned long int j=0;j<length_text;j++)
                    std::cout << local_buffer_texts_previous[j];
                std::cout << "\"\n";           
                

                //std::cout << mem_keys-total_mem_read << " " << mem_texts-total_mem_read_text << "\n";
                if((mem_keys-total_mem_read)<=0)
                    break;

                local_buffer_keys_previous=local_buffer_keys+1; //+1 to skip '\0' in the output!
                local_buffer_keys=strchr(local_buffer_keys+1, '\0' );

                
                local_buffer_texts_previous=local_buffer_texts+1; //+1 to skip '\0' in the output!
                local_buffer_texts=strchr(local_buffer_texts+1, '\0' );



            }
        }
        else
            std::cerr << "\n Build buffers! \n";
    }

   void csv_stdout_permutations(const char separator){
        char* local_buffer_keys=buffer_keys;
        char* local_buffer_texts=buffer_texts;
        char* local_buffer_keys_previous=buffer_keys;
        char* local_buffer_texts_previous=buffer_texts;        
        unsigned long int total_mem_read=0;
        unsigned long int total_mem_read_text=0;
        if(all_set){
            unsigned int buffer_permutation_size=buffer_permutation_indexes[0];
            for(unsigned int j=1;j<2*buffer_permutation_size+1;j+=2){
                unsigned long int start=buffer_permutation_indexes[j];
                unsigned long int end=buffer_permutation_indexes[j+1];
                
                for(unsigned int k=start;k<end;k++){

                    std::cout << "1980" << "-" << "03" << "-" << "16" << separator;
                    std::cout << "\"";
                    unsigned int local_key_length=buffer_keys_length[k];
                    local_buffer_keys=&buffer_keys[buffer_keys_position[k]];
                    for(unsigned int q=0;q<local_key_length-1;q++){
                          std::cout << local_buffer_keys[q];
                    }
                    
                    std::cout << "\"" << separator << "\"";
                    unsigned int local_text_length=buffer_texts_length[k];
                    local_buffer_texts=&buffer_texts[buffer_texts_position[k]];
                    for(unsigned int q=0;q<local_text_length-1;q++){
                        std::cout << local_buffer_texts[q];
                    }
                    std::cout << "\"\n";           
                }


            }
        }
        else
            std::cerr << "\n Build buffers! \n";
    }

    
};
