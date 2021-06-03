#include "info_helper.hh"

#include <iostream>
#include <fstream>
#include <sstream>
#include <map>

#include <cassert>

namespace wnb
{

  // Class info_helper

  /// List of pointer symbols
  const char *
  info_helper::symbols[info_helper::NB_SYMBOLS] = {
    "!" ,  // 0 Antonym
    "@" ,  // 1 Hypernym
    "@i",  // 2 Instance Hypernym
    "~" ,  // 3 Hyponym
    "~i",  // 4 Instance Hyponym
    "#m",  // 5 Member holonym
    "#s",  // 6 Substance holonym
    "#p",  // 7 Part holonym
    "%m",  // 8 Member meronym
    "%s",  // 9 Substance meronym
    "%p",  // 10 Part meronym
    "=" ,  // 11 Attribute
    "+" ,  // 12 Derivationally related form
    ";c",  // 13 Domain of synset - TOPIC
    "-c",  // 14 Member of this domain - TOPIC
    ";r",  // 15 Domain of synset - REGION
    "-r",  // 16 Member of this domain - REGION
    ";u",  // 17 Domain of synset - USAGE
    "-u",  // 18 Member of this domain - USAGE

    //The pointer_symbol s for verbs are:
    "*",   // 19 Entailment
    ">",   // 20 Cause
    "^",   // 21 Also see
    "$",   // 22 Verb Group

    //The pointer_symbol s for adjectives are:
    "&",   // 23 Similar to
    "<",   // 24 Participle of verb
    "\\",  // 25 Pertainym (pertains to noun)
    "=",   // 26 Attribute
  };

  const std::string info_helper::sufx[] = {
    /* Noun suffixes */
    "s", "ses", "xes", "zes", "ches", "shes", "men", "ies",
    /* Verb suffixes */
    "s", "ies", "es", "es", "ed", "ed", "ing", "ing",
    /* Adjective suffixes */
    "er", "est", "er", "est"
  };

  const std::string info_helper::addr[] = {
    /* Noun endings */
    "", "s", "x", "z", "ch", "sh", "man", "y",
    /* Verb endings */
    "", "y", "e", "", "e", "", "e", "",
    /* Adjective endings */
    "", "", "e", "e"
  };

  const int info_helper::offsets[info_helper::NUMPARTS] = { 0, 0, 8, 16, 0, 0 };
  const int info_helper::cnts[info_helper::NUMPARTS]    = { 0, 8, 8, 4, 0, 0 };

  void
  info_helper::update_pos_maps()
  {
    // http://wordnet.princeton.edu/wordnet/man/wndb.5WN.html#sect3

    indice_offset[UNKNOWN] = 0;

    indice_offset[N] = 0;
    indice_offset[V] = indice_offset[N] + pos_maps[N].size();
    indice_offset[A] = indice_offset[V] + pos_maps[V].size();
    indice_offset[R] = indice_offset[A] + pos_maps[A].size();
    indice_offset[S] = indice_offset[R] + pos_maps[R].size();

  }

  int info_helper::compute_indice(int offset, pos_t pos)
  {
    if (pos == S)
      pos = A;
    std::map<int,int>& map = pos_maps[pos];

    assert(pos <= 5 && pos > 0);

    return indice_offset[pos] + map[offset];
  }

  // Function definitions

  // Return relation between synset indices and offsets
  static
  std::map<int,int>
  preprocess_data(const std::string& fn)
  {
    std::map<int,int> map;
    std::ifstream file(fn.c_str());
    if (!file.is_open())
      throw std::runtime_error("preprocess_data: File not found: " + fn);

    std::string row;

    //skip header
    const unsigned int header_nb_lines = 29;
    for(std::size_t i = 0; i < header_nb_lines; i++)
      std::getline(file, row);

    int ind = 0;
    //parse data line
    while (std::getline(file, row))
    {
      std::stringstream srow(row);
      int offset;
      srow >> offset;
      map.insert(std::pair<int,int>(offset, ind));
      ind++;
    }

    file.close();
    return map;
  }

  info_helper
  preprocess_wordnet(const std::string& dn)
  {
    info_helper info;

    info.pos_maps[N] = preprocess_data((dn + "data.noun")); // noun_map
    info.pos_maps[V] = preprocess_data((dn + "data.verb")); // verb_map
    info.pos_maps[A] = preprocess_data((dn + "data.adj"));  // adj_map
    info.pos_maps[R] = preprocess_data((dn + "data.adv"));  // adv_map

    info.update_pos_maps();

    return info;
  }

} // end of namespace wnb

