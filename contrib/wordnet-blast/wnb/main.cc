
#include <iostream>
#include <fstream>
#include <sstream>

#include <boost/progress.hpp>
#include <boost/algorithm/string.hpp>

#include <wnb/core/wordnet.hh>
#include <wnb/core/load_wordnet.hh>
#include <wnb/core/info_helper.hh>
#include <wnb/nltk_similarity.hh>
#include <wnb/std_ext.hh>

using namespace wnb;
using namespace boost;
using namespace boost::algorithm;

bool usage(int argc, char ** argv)
{
  std::string dir;
  if (argc >= 2)
    dir = std::string(argv[1]);
  if (argc != 3 || dir[dir.length()-1] != '/')
  {
    std::cout << argv[0] << " .../wordnet_dir/ word_list_file" << std::endl;
    return true;
  }
  return false;
}

struct ws
{
  std::string w;
  float       s;

  bool operator<(const ws& a) const {return s > a.s;}
};


/// Compute similarity of word with words in word list
std::vector<ws>
compute_similarities(wordnet& wn,
                     const std::string& word,
                     const std::vector<std::string>& word_list)
{
  std::vector<ws> wslist;
  std::vector<synset> synsets1 = wn.get_synsets(word);

  for (unsigned i = 0; i < synsets1.size(); i++)
    for (unsigned k = 0; k < synsets1[i].words.size(); k++)
      std::cout << " - " << synsets1[i].words[k] << std::endl;

  nltk_similarity path_similarity(wn);
  {
    progress_timer t;
    progress_display show_progress(word_list.size());

    for (unsigned k = 0; k < word_list.size(); k++)
    {
      const std::string& w = word_list[k];
      float max = 0;
      std::vector<synset> synsets2 = wn.get_synsets(w);
      for (unsigned i = 0; i < synsets1.size(); i++)
      {
        for (unsigned j = 0; j < synsets2.size(); j++)
        {
          float s = path_similarity(synsets1[i], synsets2[j], 6);
          if (s > max)
            max = s;
        }
      }
      ws e = {w, max};
      wslist.push_back(e);
      ++show_progress;
    }
  }

  return wslist;
}

void similarity_test(wordnet&                  wn,
                     const std::string&        word,
                     std::vector<std::string>& word_list)
{
  std::vector<ws> wslist = compute_similarities(wn, word, word_list);

  std::stable_sort(wslist.begin(), wslist.end());
  for (unsigned i = 0; i < std::min(wslist.size(), size_t(10)); i++)
    std::cout << wslist[i].w << " " << wslist[i].s << std::endl;
}

void print_synsets(pos_t pos, wnb::index& idx, wordnet& wn)
{
  std::string& mword = idx.lemma;
  std::cout << "\nOverview of " << get_name_from_pos(pos) << " " << mword << "\n\n";
  std::cout << "The " << get_name_from_pos(pos) << " " << mword << " has "
            << idx.synset_ids.size() << ((idx.synset_ids.size() == 1) ? " sense": " senses");

  if (idx.tagsense_cnt != 0)
    std::cout << " (first " << idx.tagsense_cnt << " from tagged texts)";
  else
    std::cout << " (no senses from tagged texts)";

  std::cout << "\n";
  std::cout << "                                      \n";

  for (std::size_t i = 0; i < idx.synset_ids.size(); i++)
  {
    int id = idx.synset_ids[i];
    const synset& synset = wn.wordnet_graph[id];

    std::cout << i+1 << ". ";
    for (std::size_t k = 0; k < synset.tag_cnts.size(); k++)
    {
      if (synset.tag_cnts[k].first == mword)
        std::cout << "(" << synset.tag_cnts[k].second << ") ";
    }

    std::vector<std::string> nwords;
    for (auto& w : synset.words)
      nwords.push_back((pos == A) ? w.substr(0, w.find_first_of("(")) : w);

    std::cout << replace_all_copy(join(nwords, ", "), "_", " ");
    std::cout << " -- (" << trim_copy(synset.gloss) << ")";
    std::cout << std::endl;
  }
}

void wn_like(wordnet& wn, const std::string& word, pos_t pos)
{
  if (word == "")
    return;

  typedef std::vector<wnb::index> vi;
  std::pair<vi::iterator,vi::iterator> bounds = wn.get_indexes(word);

  for (vi::iterator it = bounds.first; it != bounds.second; it++)
  {
    if (pos != -1 && it->pos == pos)
    {
      print_synsets(pos, *it, wn);
    }
  }
}

void batch_test(wordnet& wn, std::vector<std::string>& word_list)
{
  for (std::size_t i = 0; i < word_list.size(); i++)
  {
    for (unsigned p = 1; p < POS_ARRAY_SIZE; p++)
    {
      pos_t pos = (pos_t) p;

      wn_like(wn, word_list[i], pos);
      std::string mword = wn.morphword(word_list[i], pos);
      if (mword != word_list[i])
        wn_like(wn, mword, pos);
    }
  }
}

int main(int argc, char ** argv)
{
  if (usage(argc, argv))
    return 1;

  // read command line
  std::string wordnet_dir = argv[1];
  std::string test_file   = argv[2];

  wordnet wn(wordnet_dir);

  // read test file
  std::string list = ext::read_file(test_file);
  std::vector<std::string> wl        =  ext::split(list);

  batch_test(wn, wl);
}

