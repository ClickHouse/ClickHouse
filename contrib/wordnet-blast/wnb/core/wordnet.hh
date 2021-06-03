#pragma once

# include <iostream>
# include <string>
# include <cassert>
# include <vector>
//# include <boost/filesystem.hpp>

//Possible https://bugs.launchpad.net/ubuntu/+source/boost/+bug/270873
# include <boost/graph/graph_traits.hpp>
# include <boost/graph/adjacency_list.hpp>

# include "load_wordnet.hh"
# include "pos_t.hh"

namespace wnb
{

  /// More info here: http://wordnet.princeton.edu/wordnet/man/wndb.5WN.html

  struct info_helper;

  /// Synset
  struct synset
  {
    int  lex_filenum;
    std::size_t  w_cnt;
    std::vector<std::string> words;
    std::vector<int> lex_ids;
    std::size_t p_cnt;
    std::string gloss;

    // extra
    pos_t pos;        ///< pos (replace ss_type)
    int id;           ///< unique identifier (replace synset_offset)
    int sense_number; ///< http://wordnet.princeton.edu/man/senseidx.5WN.html
    std::vector<std::pair<std::string, int> > tag_cnts; ///< http://wordnet.princeton.edu/man/senseidx.5WN.html

    bool operator==(const synset& s) const { return (id == s.id);  }
    bool operator<(const synset& s) const { return (id < s.id);   }
  };


  /// Rel between synsets properties
  struct ptr
  {
    //std::string pointer_symbol; ///< symbol of the relation
    int pointer_symbol;
    int source; ///< source word inside synset
    int target; ///< target word inside synset
  };


  /// Index
  struct index
  {
    std::string lemma;

    std::size_t synset_cnt;
    std::size_t p_cnt;
    std::size_t sense_cnt;
    float       tagsense_cnt;
    std::vector<std::string> ptr_symbols;
    std::vector<int>         synset_offsets;

    // extra
    std::vector<int> synset_ids;
    pos_t pos;

    bool operator<(const index& b) const
    {
      return (lemma.compare(b.lemma) < 0);
    }
  };


  /// Wordnet interface class
  struct wordnet
  {
    typedef boost::adjacency_list<boost::vecS, boost::vecS,
                                  boost::directedS,
                                  synset, ptr> graph; ///< boost graph type

    /// Constructor
    wordnet(const std::string& wordnet_dir, bool verbose=false);

    /// Return synsets matching word
    std::vector<synset> get_synsets(const std::string& word, pos_t pos = pos_t::UNKNOWN);
    //FIXME: todo
    std::vector<synset> get_synset(const std::string& word, char pos, int i);
    // added
    const std::vector<std::string> * get_synset(const std::string& word, pos_t pos = pos_t::UNKNOWN) const;

    std::pair<std::vector<index>::iterator, std::vector<index>::iterator>
    get_indexes(const std::string& word);

    std::pair<std::vector<index>::const_iterator, std::vector<index>::const_iterator>
    get_indexes_const(const std::string& word) const;

    std::string wordbase(const std::string& word, int ender);

    std::string morphword(const std::string& word, pos_t pos);

    std::vector<index> index_list;    ///< index list // FIXME: use a map
    graph              wordnet_graph; ///< synsets graph
    info_helper        info;          ///< helper object
    bool               _verbose;

    typedef std::map<std::string,std::string> exc_t;
    std::map<pos_t, exc_t> exc;
  };

} // end of namespace wnb
