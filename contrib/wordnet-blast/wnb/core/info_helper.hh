#pragma once

# include <string>
# include <stdexcept>
# include <map>

# include "pos_t.hh"

namespace wnb
{

  /// Useful information for wordnet in-memory import
  struct info_helper
  {
    /// Symbols' size
    static const std::size_t NB_SYMBOLS = 27;
    static const std::size_t NUMPARTS = POS_ARRAY_SIZE;

    /// List of pointer symbols
    static const char *      symbols[NB_SYMBOLS];
    static const std::string sufx[];
    static const std::string addr[];

    static const int  offsets[NUMPARTS];
    static const int  cnts[NUMPARTS];

    typedef std::map<int,int>       i2of_t;     ///< indice/offset correspondences
    typedef std::map<pos_t, i2of_t> pos_i2of_t; ///< pos / map  correspondences

    /// Constructor
    info_helper() { update_pos_maps(); }

    /// Compute the number of synsets (i.e. the number of vertex in the graph)
    unsigned nb_synsets()
    {
      typedef pos_i2of_t::iterator iter_t;

      int sum = 0;
      for (iter_t it = pos_maps.begin(); it != pos_maps.end(); it++)
        sum += (*it).second.size();

      return sum;
      //return adj_map.size() + adv_map.size() + noun_map.size() + verb_map.size();
    }

    // Given a pos return the starting indice in the graph
    int get_indice_offset(pos_t pos)
    {
      return indice_offset[pos];
    }

    /// Helper function computing global indice in graph from local offset
    int compute_indice(int offset, pos_t pos);

    /// Update a map allowing one to get the correct map given a pos
    void update_pos_maps();

    int get_symbol(const std::string& ps)
    {
      for (unsigned i = 0; i < NB_SYMBOLS; i++)
        if (ps == symbols[i])
          return i;
      throw std::runtime_error("Symbol NOT FOUND.");
    }

    pos_t get_pos(const char& c)
    {
      return get_pos_from_char(c);
    }

  public:

    // i2of_t adj_map;
    // i2of_t adv_map;
    // i2of_t noun_map;
    // i2of_t verb_map;

    pos_i2of_t  pos_maps;
    std::size_t indice_offset[POS_ARRAY_SIZE];
  };

  /// Create a new info_help based on wordnet data located in dn (../dict/)
  info_helper preprocess_wordnet(const std::string& dn);

} // end of namespace wncpp
