#include <wnb/core/wordnet.hh>
#include <wnb/std_ext.hh>

#include <string>
#include <set>
#include <algorithm>
#include <stdexcept>
#include <boost/graph/breadth_first_search.hpp>
#include <boost/graph/filtered_graph.hpp>

namespace wnb
{

  //FIXME: Make (smart) use of fs::path
  wordnet::wordnet(const std::string& wordnet_dir, bool verbose)
    : _verbose(verbose)
  {
    if (_verbose)
    {
      std::cout << wordnet_dir << std::endl;
    }

    info = preprocess_wordnet(wordnet_dir);

    wordnet_graph = graph(info.nb_synsets());
    load_wordnet(wordnet_dir, *this, info);

    if (_verbose)
    {
      std::cout << "nb_synsets: " << info.nb_synsets() << std::endl;
    }
    //FIXME: this check is only valid for Wordnet 3.0
    //assert(info.nb_synsets() == 142335);//117659);
    assert(info.nb_synsets() > 0);
  }

  std::vector<synset>
  wordnet::get_synsets(const std::string& word, pos_t pos)
  {
    std::vector<synset> synsets;

    // morphing
    std::string mword = morphword(word, pos);
    if (mword == "")
      return synsets;

    // binary_search
    typedef std::vector<index> vi;
    std::pair<vi::iterator,vi::iterator> bounds = get_indexes(mword);

    vi::iterator it;
    for (it = bounds.first; it != bounds.second; it++)
    {
      if (pos == pos_t::UNKNOWN || it->pos == pos)
      {
        for (std::size_t i = 0; i < it->synset_ids.size(); i++)
        {
          int id = it->synset_ids[i];
          synsets.push_back(wordnet_graph[id]);
        }
      }
    }

    return synsets;
  }

  const std::vector<std::string> *
  wordnet::get_synset(const std::string& word, pos_t pos) const {
    
    typedef std::vector<index> vi;
    std::pair<vi::const_iterator,vi::const_iterator> bounds = get_indexes_const(word);

    for (vi::const_iterator it = bounds.first; it != bounds.second; it++)
    {
      if (pos == pos_t::UNKNOWN || it->pos == pos)
      {
        int id = it->synset_ids[0];
        return &wordnet_graph[id].words;
      }
    }
    return nullptr;
  }

  std::pair<std::vector<index>::const_iterator, std::vector<index>::const_iterator>
  wordnet::get_indexes_const(const std::string& word) const
  {
    index light_index;
    light_index.lemma = word;

    typedef std::vector<index> vi;
    std::pair<vi::const_iterator,vi::const_iterator> bounds =
      std::equal_range(index_list.begin(), index_list.end(), light_index);

    return bounds;
  }

  std::pair<std::vector<index>::iterator, std::vector<index>::iterator>
  wordnet::get_indexes(const std::string& word)
  {
    index light_index;
    light_index.lemma = word;

    typedef std::vector<index> vi;
    std::pair<vi::iterator,vi::iterator> bounds =
      std::equal_range(index_list.begin(), index_list.end(), light_index);

    return bounds;
  }

  std::string
  wordnet::wordbase(const std::string& word, int ender)
  {
    if (ext::ends_with(word, info.sufx[ender]))
    {
      int sufxlen = info.sufx[ender].size();
      std::string strOut = word.substr(0, word.size() - sufxlen);
      if (!info.addr[ender].empty())
        strOut += info.addr[ender];
      return strOut;
    }
    return word;
  }

  bool is_defined(const std::string& word, pos_t pos)
  {
    // hack FIXME: Some verbs are built with -e suffix ('builde' is just an example).
    if (pos == V && word == "builde")
      return false;
    return true;
  }

  // Try to find baseform (lemma) of individual word in POS
  std::string
  wordnet::morphword(const std::string& word, pos_t pos)
  {
    // first look for word on exception list
    exc_t::iterator it = exc[pos].find(word);
    if (it != exc[pos].end())
      return it->second; // found in exception list

    std::string tmpbuf;
    std::string end;
    int cnt = 0;

    if (pos == R)
      return ""; // Only use exception list for adverbs

    if (pos == N)
    {
      if (ext::ends_with(word, "ful"))
      {
        cnt = word.size() - 3;
        tmpbuf = word.substr(0, cnt);
        end = "ful";
      }
      else
      {
        // check for noun ending with 'ss' or short words
        if (ext::ends_with(word, "ss") || word.size() <= 2)
          return "";
      }
    }

    // If not in exception list, try applying rules from tables

    if (tmpbuf.size() == 0)
      tmpbuf = word;

    if (pos != pos_t::UNKNOWN) 
    {
      int offset  = info.offsets[pos];
      int pos_cnt = info.cnts[pos];

      std::string morphed;
      for  (int i = 0; i < pos_cnt; i++)
      {
        morphed = wordbase(tmpbuf, (i + offset));
        if (morphed != tmpbuf && is_defined(morphed, pos))
           return morphed + end;
      }
      return morphed;
    }
    return word;
  }

} // end of namespace wnb
