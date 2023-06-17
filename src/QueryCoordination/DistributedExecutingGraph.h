//#pragma once
//
//#include <base/types.h>
//#include <QueryCoordination/PlanFragment.h>
//
//namespace DB
//{
//using Hosts = std::vector<String>;
//
//using HostToFragments = std::unordered_map<String, PlanFragmentPtrs>;
//using FragmentToHosts = std::unordered_map<FragmentID , Hosts>;
//
//
//class DistributedExecuteGraph
//{
//public:
//
//    struct DirectEdge
//    {
//        String to_host;
//        FragmentID to_fragment;
//        PlanID to_exchange;
//    };
//
//    /// data stream opposite direction
//    struct BackEdge
//    {
//        String to_host;
//        FragmentID to_fragment;
//    };
//
//    using DirectEdges = std::list<DirectEdge>;
//    using BackEdges = std::list<BackEdge>;
//
//    struct GraphNode
//    {
//        String host;
//        PlanFragmentPtr fragment;
//        PlanID exchange_id;
//
//        /// Direct edges are for output ports(exchange data node), back edges are for input ports(data sink).
//        DirectEdges direct_edges;
//        BackEdges back_edges;
//    };
//
//    using GraphNodes = std::vector<GraphNode>;
//
//    void addNode(String host, PlanFragmentPtr fragment)
//    {
//        nodes.emplace_back(GraphNode{.host = host, .fragment = fragment});
//    }
//
//    void addEdges(uint64_t node)
//    {
//        GraphNode & from = nodes[node];
//
//        bool was_edge_added = false;
//
//        /// Add backward edges from input ports.
//        const auto & inputs_children = from.fragment->getChildren();
//
//        auto from_input = from.back_edges.size();
//
//        if (from_input < inputs_children.size())
//        {
//            was_edge_added = true;
//
//            for (auto it = std::next(inputs_children.begin(), from_input); it != inputs_children.end(); ++it, ++from_input)
//            {
//                FragmentID to_fragment = (*it)->getFragmentId();
//                Hosts & hosts = fragment_hosts[to_fragment];
//                auto output_port_number = to->getOutputPortNumber(&it->getOutputPort());
//                Edge edge(0, true, from_input, output_port_number, &nodes[node]->post_updated_input_ports);
//                auto & added_edge = addEdge(nodes[node]->back_edges, std::move(edge), from, to);
//                it->setUpdateInfo(&added_edge.update_info);
//            }
//        }
//
//        /// Add direct edges from output ports.
//        auto & outputs = from->getOutputs();
//        auto from_output = nodes[node]->direct_edges.size();
//
//        if (from_output < outputs.size())
//        {
//            was_edge_added = true;
//
//            for (auto it = std::next(outputs.begin(), from_output); it != outputs.end(); ++it, ++from_output)
//            {
//                const IProcessor * to = &it->getInputPort().getProcessor();
//                auto input_port_number = to->getInputPortNumber(&it->getInputPort());
//                Edge edge(0, false, input_port_number, from_output, &nodes[node]->post_updated_output_ports);
//                auto & added_edge = addEdge(nodes[node]->direct_edges, std::move(edge), from, to);
//                it->setUpdateInfo(&added_edge.update_info);
//            }
//        }
//
//        return was_edge_added;
//    }
//
//
//private:
//    GraphNodes nodes;
//    HostToFragments host_fragments;
//    FragmentToHosts fragment_hosts;
//    std::unordered_map<FragmentID , PlanFragmentPtr> id_fragment;
//};
//
//}
//
