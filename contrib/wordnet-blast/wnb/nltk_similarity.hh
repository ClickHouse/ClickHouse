#ifndef _NLTK_SIMILARITY_HH
# define _NLTK_SIMILARITY_HH

# include <queue>
# include <boost/graph/filtered_graph.hpp>
# include <wnb/core/wordnet.hh>

namespace wnb
{
  namespace internal
  {

    //Helper class filtering out other than hypernym relations
    template <typename PointerSymbolMap>
    struct hyper_edge
    {
      hyper_edge() { }

      hyper_edge(PointerSymbolMap pointer_symbol)
        : m_pointer_symbol(pointer_symbol) { }

      template <typename Edge>
      bool operator()(const Edge& e) const
      {
        int p_s = get(m_pointer_symbol, e);
        return p_s == 1; // hypernyme (instance_hypernyme not used here)
      }

      PointerSymbolMap m_pointer_symbol;
    };

  } // end of anonymous namespace


  class nltk_similarity
  {

    typedef boost::property_map<wordnet::graph,
                                int ptr::*>::type PointerSymbolMap;
    typedef boost::filtered_graph<wordnet::graph,
                                  internal::hyper_edge<PointerSymbolMap> > G;
    typedef boost::graph_traits<G>::vertex_descriptor vertex;

    internal::hyper_edge<PointerSymbolMap> filter;
    G fg;

  public:

    nltk_similarity(wordnet& wn)
      : filter(get(&ptr::pointer_symbol, wn.wordnet_graph)),
                   fg(wn.wordnet_graph, filter)
    { }

    /// Get list of hypernyms of s along with distance to s
    std::map<vertex, int> hypernym_map(vertex s);

    /// Get shortest path between and synset1 and synset2.
    int shortest_path_distance(const synset& synset1, const synset& synset2);

    /// return disance
    float operator()(const synset& synset1, const synset& synset2, int=0);

  };

  std::map<nltk_similarity::vertex, int>
  nltk_similarity::hypernym_map(nltk_similarity::vertex s)
  {
    std::map<vertex, int> map;

    // Python:
    // for (hypernym in self[HYPERNYM])
    //   distances |= hypernym.hypernym_distances(distance+1);

    boost::graph_traits<G>::out_edge_iterator e, e_end;
    std::queue<vertex> q;

    q.push(s);
    map[s] = 0;
    while (!q.empty())
    {
      vertex u = q.front(); q.pop();

      int new_d = map[u] + 1;
      for (boost::tuples::tie(e, e_end) = out_edges(u, fg); e != e_end; ++e)
      {
        vertex v = target(*e,fg);
        q.push(v);

        if (map.find(v) != map.end())
        {
          if (new_d < map[v])
            map[v] = new_d;
          else
            q.pop();
        }
        else
          map[v] = new_d;
      }
    }

    return map;
  }


  int
  nltk_similarity::shortest_path_distance(const synset& synset1, const synset& synset2)
  {
    vertex v1 = synset1.id;
    vertex v2 = synset2.id;

    std::map<vertex, int> map1 = hypernym_map(v1);
    std::map<vertex, int> map2 = hypernym_map(v2);

    // For each ancestor synset common to both subject synsets, find the
    // connecting path length. Return the shortest of these.

    int path_distance = -1;
    std::map<vertex, int>::iterator it, it2;
    for (it = map1.begin(); it != map1.end(); it++)
      for (it2 = map2.begin(); it2 != map2.end(); it2++)
        if (fg[it->first] == fg[it2->first])
        {
          int new_distance = it->second + it2->second;
          if (path_distance < 0 || new_distance < path_distance)
            path_distance = new_distance;
        }

    return path_distance;
  }


  float
  nltk_similarity::operator()(const synset& synset1, const synset& synset2, int)
  {
    int distance = shortest_path_distance(synset1, synset2);
    if (distance >= 0)
      return 1. / (distance + 1);
    else
      return -1;
  }


} // end of namespace wnb

#endif /* _NLTK_SIMILARITY_HH */

