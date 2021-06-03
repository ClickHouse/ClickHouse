#pragma once

namespace wnb
{

  static const std::size_t POS_ARRAY_SIZE = 6;
  static const char POS_ARRAY[POS_ARRAY_SIZE] = {'u', 'n', 'v', 'a', 'r', 's'};

  enum pos_t
  	{
        UNKNOWN = 0,
        N       = 1,
        V       = 2,
        A       = 3,
        R       = 4,
        S       = 5,
  	};


  inline pos_t get_pos_from_name(const std::string& pos)
  {
    if (pos == "adj")
      return A;
    if (pos == "noun")
      return N;
    if (pos == "adv")
      return R;
    if (pos == "verb")
      return V;
    if (pos == "adj sat")
      return S;
    return UNKNOWN;
  }

  inline std::string get_name_from_pos(const pos_t& pos)
  {
    switch (pos)
    {
    case A: return "adj";
    case N: return "noun";
    case R: return "adv";
    case V: return "verb";
    case S: return "adj sat";
    default: return "UNKNOWN";
    }
  }

  inline pos_t get_pos_from_char(const char& c)
  {
    switch (c)
    {
    case 'a': return A;
    case 'n': return N;
    case 'r': return R;
    case 'v': return V;
    case 's': return S;
    default: return UNKNOWN;
    }
  }

} // end of namespace wncpp
