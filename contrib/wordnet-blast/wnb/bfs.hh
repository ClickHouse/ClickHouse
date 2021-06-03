#ifndef _BFS_HH
# define _BFS_HH

# include <boost/graph/breadth_first_search.hpp>
# include <boost/graph/filtered_graph.hpp>

namespace wnb
{
  struct synset;

  namespace bfs // breadth first search tools
  {
    /// bfs_visitor
    /// Sum distances and throw answer if target synset found
    template <typename DistanceMap>
    class distance_recorder : public boost::default_bfs_visitor
    {
    public:
      distance_recorder(DistanceMap dist, const synset& s, int max)
        : d(dist), target(s), max_length(max)
      { }

      template <typename Edge, typename Graph>
      void tree_edge(Edge e, const Graph& g) const
      {
        typename boost::graph_traits<Graph>::vertex_descriptor
          u = boost::source(e, g), v = boost::target(e, g);
        d[v] = d[u] + 1;

        if (g[v] == target)
          throw d[v];
        if (d[v] > max_length)
          throw -1;
      }
    private:
      DistanceMap d;
      const synset& target;
      int max_length;
    };

    /// Convenience function
    template <typename DistanceMap>
    distance_recorder<DistanceMap>
    record_distance(DistanceMap d, const synset& s, int m)
    {
      return distance_recorder<DistanceMap>(d, s, m);
    }

    /// This predicate function object determines which edges of the original
    /// graph will show up in the filtered graph.
    //FIXME: Do we really need a map here (check cost of property_map construction 
    // / should be light)
    template <typename PointerSymbolMap>
    struct hypo_hyper_edge {
      hypo_hyper_edge() { }
      hypo_hyper_edge(PointerSymbolMap pointer_symbol)
        : m_pointer_symbol(pointer_symbol) { }
      template <typename Edge>
      bool operator()(const Edge& e) const {
        int p_s = get(m_pointer_symbol, e);
        //see pointer symbol list in info_helper.hh
        return p_s == 1 || p_s == 2 || p_s == 3 || p_s == 4; 
      }
      PointerSymbolMap m_pointer_symbol;
    };

  } // end of wnb::bfs

} // end of namespace wnb

#endif /* _BFS_HH */

