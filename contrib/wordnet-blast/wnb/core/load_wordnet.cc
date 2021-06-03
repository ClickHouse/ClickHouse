#include "load_wordnet.hh"

#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include <utility>

#include <boost/graph/adjacency_list.hpp>
#include <boost/progress.hpp>
#include <boost/algorithm/string.hpp>

#include <wnb/std_ext.hh>

#include "wordnet.hh"
#include "info_helper.hh"
#include "pos_t.hh"

namespace bg = boost::graph;

namespace wnb
{

  namespace
  {

    // Load synset's words
    void load_data_row_words(std::stringstream& srow, synset& synset)
    {
      srow >> std::hex >> synset.w_cnt >> std::dec;
      for (std::size_t i = 0; i < synset.w_cnt; i++)
      {
        //word lex_id

        std::string word;
        srow >> word;
        synset.words.push_back(word);

        int lex_id;
        srow >> std::hex >> lex_id >> std::dec;
        synset.lex_ids.push_back(lex_id);
      }
    }

    // Add rel to graph
    void add_wordnet_rel(std::string& pointer_symbol_,// type of relation
                         int synset_offset,           // dest offset
                         pos_t pos,                   // p.o.s. of dest
                         int src,                     // word src
                         int trgt,                    // word target
                         synset& synset,              // source synset
                         wordnet& wn,                 // our wordnet
                         info_helper& info)           // helper
    {
      //if (pos == S || synset.pos == S)
      //  return; //FIXME: check where are s synsets.

      int u = synset.id;
      int v = info.compute_indice(synset_offset, pos);

      ptr p;
      p.pointer_symbol = info.get_symbol(pointer_symbol_);
      p.source = src;
      p.target = trgt;

      boost::add_edge(u,v, p, wn.wordnet_graph);
    }


    // load ptrs
    void load_data_row_ptrs(std::stringstream& srow, synset& synset,
                            wordnet& wn, info_helper& info)
    {
      srow >> synset.p_cnt;
      for (std::size_t i = 0; i < synset.p_cnt; i++)
      {
        //http://wordnet.princeton.edu/wordnet/man/wndb.5WN.html#sect3
        //pointer_symbol  synset_offset  pos  source/target
        std::string pointer_symbol_;
        int   synset_offset;
        pos_t pos;
        int   src;
        int   trgt;

        srow >> pointer_symbol_;
        srow >> synset_offset;

        char c;
        srow >> c;
        pos = info.get_pos(c);

        //print extracted edges
        //std::cout << "(" << pointer_symbol << ", " << synset_offset;
        //std::cout << ", " << pos << ")" << std::endl;

        // Extract source/target words info
        std::string src_trgt;
        srow >> src_trgt;
        std::stringstream ssrc(std::string(src_trgt,0,2));
        std::stringstream strgt(std::string(src_trgt,2,2));
        ssrc >> std::hex >> src >> std::dec;
        strgt >> std::hex >> trgt >> std::dec;

        add_wordnet_rel(pointer_symbol_, synset_offset, pos, src, trgt, synset, wn, info);
      }
    }


    // Load a synset and add it to the wordnet class.
    void load_data_row(const std::string& row, wordnet& wn, info_helper& info)
    {
      //http://wordnet.princeton.edu/wordnet/man/wndb.5WN.html#sect3
      // synset_offset lex_filenum ss_type w_cnt word lex_id [word lex_id...] p_cnt [ptr...] [frames...] | gloss
      synset synset;

      std::stringstream srow(row);
      int synset_offset;
      srow >> synset_offset;
      srow >> synset.lex_filenum;
      char ss_type;
      srow >> ss_type;

      // extra information
      synset.pos = info.get_pos(ss_type);
      synset.id  = info.compute_indice(synset_offset, synset.pos);

      // words
      load_data_row_words(srow, synset);

      // ptrs
      load_data_row_ptrs(srow, synset, wn, info);

      //frames (skipped)
      std::string tmp;
      while (srow >> tmp)
        if (tmp == "|")
          break;

      // gloss
      std::getline(srow, synset.gloss);

      // extra
      synset.sense_number = 0;

      // Add synset to graph
      wn.wordnet_graph[synset.id] = synset;
    }


    // Parse data.noun files
    void load_wordnet_data(const std::string& fn, wordnet& wn, info_helper& info)
    {
      std::ifstream fin(fn.c_str());
      if (!fin.is_open())
        throw std::runtime_error("File missing: " + fn);

      static const int MAX_LENGTH = 20480;
      char row[MAX_LENGTH];

      //skip header
      for(unsigned i = 0; i < 29; i++)
        fin.getline(row, MAX_LENGTH);

      //parse data line
      while (fin.getline(row, MAX_LENGTH))
        load_data_row(row, wn, info);

      fin.close();
    }


    //FIXME: It seems possible to replace synset_offsets with indice here.
    void load_index_row(const std::string& row, wordnet& wn, info_helper& info)
    {
      // lemma pos synset_cnt p_cnt [ptr_symbol...] sense_cnt tagsense_cnt synset_offset [synset_offset...]
      index index;
      std::stringstream srow(row);

      char pos;
      srow >> index.lemma;
      srow >> pos;
      index.pos = info.get_pos(pos); // extra data
      srow >> index.synset_cnt;
      srow >> index.p_cnt;

      std::string tmp_p;
      for (std::size_t i = 0; i < index.p_cnt; i++)
      {
        srow >> tmp_p;
        index.ptr_symbols.push_back(tmp_p);
      }
      srow >> index.sense_cnt;
      srow >> index.tagsense_cnt;

      int tmp_o;
      while (srow >> tmp_o)
      {
        index.synset_offsets.push_back(tmp_o);
        index.synset_ids.push_back(info.compute_indice(tmp_o, index.pos)); // extra data
      }

      //add synset to index list
      wn.index_list.push_back(index);
    }


    void load_wordnet_index(const std::string& fn, wordnet& wn, info_helper& info)
    {
      std::ifstream fin(fn.c_str());
      if (!fin.is_open())
        throw std::runtime_error("File Not Found: " + fn);

      static const int MAX_LENGTH = 20480;
      char row[MAX_LENGTH];

      //skip header
      const unsigned int header_nb_lines = 29;
      for(std::size_t i = 0; i < header_nb_lines; i++)
        fin.getline(row, MAX_LENGTH);

      //parse data line
      while (fin.getline(row, MAX_LENGTH))
        load_index_row(row, wn, info);

      fin.close();
    }


    void load_wordnet_exc(const std::string& dn, std::string cat,
                          wordnet& wn, info_helper&)
    {
      std::string fn = dn + cat + ".exc";
      std::ifstream fin(fn.c_str());
      if (!fin.is_open())
        throw std::runtime_error("File Not Found: " + fn);

      std::map<std::string,std::string>& exc = wn.exc[get_pos_from_name(cat)];

      std::string row;

      std::string key, value;
      while (std::getline(fin, row))
      {
        std::stringstream srow(row);
        srow >> key;
        srow >> value;

        exc[key] = value;
      }
    }

    void load_wordnet_cat(const std::string dn, std::string cat,
                          wordnet& wn, info_helper& info)
    {
      load_wordnet_data((dn + "data." + cat), wn, info);
      load_wordnet_index((dn + "index." + cat), wn, info);
      load_wordnet_exc(dn, cat, wn, info);
    }

    // FIXME: this file is not in all packaged version of wordnet
    void load_wordnet_index_sense(const std::string& dn, wordnet& wn, info_helper& info)
    {
      std::string fn = dn + "index.sense";
      std::ifstream fin(fn.c_str());
      if (!fin.is_open())
        throw std::runtime_error("File Not Found: " + fn);

      std::string row;
      std::string sense_key;
      int synset_offset;
      while (std::getline(fin, row))
      {
        std::stringstream srow(row);
        srow >> sense_key;

        // Get the pos of the lemma
        std::vector<std::string> sk = ext::split(sense_key,'%');
        std::string word = sk.at(0);
        std::stringstream tmp(ext::split(sk.at(1), ':').at(0));
        int ss_type;
        tmp >> ss_type;
        pos_t pos =  (pos_t) ss_type;

        srow >> synset_offset;

        // Update synset info
        int u = info.compute_indice(synset_offset, pos);
        int sense_number;
        srow >> sense_number;
        wn.wordnet_graph[u].sense_number += sense_number;
        int tag_cnt;
        srow >> tag_cnt;
        if (tag_cnt != 0)
          wn.wordnet_graph[u].tag_cnts.push_back( make_pair(word,tag_cnt) );

        //if (synset_offset == 2121620)
        //  std::cout << u << " " << word << " " << synset_offset << " "
        //            <<  wn.wordnet_graph[u].tag_cnt << " "
        //            <<  wn.wordnet_graph[u].words[0] << std::endl;
      }
    }

    // wn -over used info in cntlist even if this is deprecated
    // It is ok not to FIX and use this function
    void load_wordnet_cntlist(const std::string& dn, wordnet& wn, info_helper& info)
    {
      std::string fn = dn + "cntlist";
      std::ifstream fin(fn.c_str());
      if (!fin.is_open())
        throw std::runtime_error("File Not Found: " + fn);

      std::string sense_key;
      int sense_number;
      int tag_cnt;

      std::string row;
      while (std::getline(fin, row))
      {
        std::stringstream srow(row);

        srow >> sense_key;
        srow >> sense_number;
        srow >> tag_cnt;

        // Get the pos of the lemma
        std::string word = ext::split(sense_key,'%').at(0);
        std::stringstream tmp(ext::split(ext::split(sense_key,'%').at(1), ':').at(0));
        int ss_type;
        tmp >> ss_type;
        pos_t pos = (pos_t) ss_type;

        // Update synset info
        int synset_offset; // FIXME
        int u = info.compute_indice(synset_offset, pos);
        wn.wordnet_graph[u].sense_number += sense_number;
        if (tag_cnt != 0)
          wn.wordnet_graph[u].tag_cnts.push_back( make_pair(word,tag_cnt) );
      }
    }

  } // end of anonymous namespace

  void load_wordnet(const std::string& dn, wordnet& wn, info_helper& info)
  {
    // vertex added in this order a n r v

    std::string fn = dn;

    if (wn._verbose)
    {
      std::cout << std::endl;
      std::cout << "### Loading Wordnet 3.0";
      boost::progress_display show_progress(5);
      boost::progress_timer t;

      load_wordnet_cat(dn, "adj", wn, info);
      ++show_progress;
      load_wordnet_cat(dn, "noun", wn, info);
      ++show_progress;
      load_wordnet_cat(dn, "adv", wn, info);
      ++show_progress;
      load_wordnet_cat(dn, "verb", wn, info);
      ++show_progress;
      load_wordnet_index_sense(dn, wn, info);
      ++show_progress;
      std::cout << std::endl;
    }
    else
    {
      load_wordnet_cat(dn, "adj", wn, info);
      load_wordnet_cat(dn, "noun", wn, info);
      load_wordnet_cat(dn, "adv", wn, info);
      load_wordnet_cat(dn, "verb", wn, info);
      load_wordnet_index_sense(dn, wn, info);
    }

    std::stable_sort(wn.index_list.begin(), wn.index_list.end());
  }

} // end of namespace wnb
