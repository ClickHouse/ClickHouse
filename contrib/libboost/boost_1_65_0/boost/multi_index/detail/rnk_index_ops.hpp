/* Copyright 2003-2017 Joaquin M Lopez Munoz.
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *
 * See http://www.boost.org/libs/multi_index for library home page.
 */

#ifndef BOOST_MULTI_INDEX_DETAIL_RNK_INDEX_OPS_HPP
#define BOOST_MULTI_INDEX_DETAIL_RNK_INDEX_OPS_HPP

#if defined(_MSC_VER)
#pragma once
#endif

#include <boost/config.hpp> /* keep it first to prevent nasty warns in MSVC */
#include <boost/mpl/and.hpp>
#include <boost/multi_index/detail/promotes_arg.hpp>
#include <cstddef>
#include <utility>

namespace boost{

namespace multi_index{

namespace detail{

/* Common code for ranked_index memfuns having templatized and
 * non-templatized versions.
 */

template<typename Pointer>
inline std::size_t ranked_node_size(Pointer x)
{
  return x!=Pointer(0)?x->size:0;
}

template<typename Pointer>
inline Pointer ranked_index_nth(std::size_t n,Pointer end_)
{
  Pointer top=end_->parent();
  if(top==Pointer(0)||n>=top->size)return end_;

  for(;;){
    std::size_t s=ranked_node_size(top->left());
    if(n==s)return top;
    if(n<s)top=top->left();
    else{
      top=top->right();
      n-=s+1;
    }
  }
}

template<typename Pointer>
inline std::size_t ranked_index_rank(Pointer x,Pointer end_)
{
  Pointer top=end_->parent();
  if(top==Pointer(0))return 0;
  if(x==end_)return top->size;

  std::size_t s=ranked_node_size(x->left());
  while(x!=top){
    Pointer z=x->parent();
    if(x==z->right()){
      s+=ranked_node_size(z->left())+1;
    }
    x=z;
  }
  return s;
}

template<
  typename Node,typename KeyFromValue,
  typename CompatibleKey,typename CompatibleCompare
>
inline std::size_t ranked_index_find_rank(
  Node* top,Node* y,const KeyFromValue& key,const CompatibleKey& x,
  const CompatibleCompare& comp)
{
  typedef typename KeyFromValue::result_type key_type;

  return ranked_index_find_rank(
    top,y,key,x,comp,
    mpl::and_<
      promotes_1st_arg<CompatibleCompare,CompatibleKey,key_type>,
      promotes_2nd_arg<CompatibleCompare,key_type,CompatibleKey> >());
}

template<
  typename Node,typename KeyFromValue,
  typename CompatibleCompare
>
inline std::size_t ranked_index_find_rank(
  Node* top,Node* y,const KeyFromValue& key,
  const BOOST_DEDUCED_TYPENAME KeyFromValue::result_type& x,
  const CompatibleCompare& comp,mpl::true_)
{
  return ranked_index_find_rank(top,y,key,x,comp,mpl::false_());
}

template<
  typename Node,typename KeyFromValue,
  typename CompatibleKey,typename CompatibleCompare
>
inline std::size_t ranked_index_find_rank(
  Node* top,Node* y,const KeyFromValue& key,const CompatibleKey& x,
  const CompatibleCompare& comp,mpl::false_)
{
  if(!top)return 0;

  std::size_t s=top->impl()->size,
              s0=s;
  Node*       y0=y;

  do{
    if(!comp(key(top->value()),x)){
      y=top;
      s-=ranked_node_size(y->right())+1;
      top=Node::from_impl(top->left());
    }
    else top=Node::from_impl(top->right());
  }while(top);
    
  return (y==y0||comp(x,key(y->value())))?s0:s;
}

template<
  typename Node,typename KeyFromValue,
  typename CompatibleKey,typename CompatibleCompare
>
inline std::size_t ranked_index_lower_bound_rank(
  Node* top,Node* y,const KeyFromValue& key,const CompatibleKey& x,
  const CompatibleCompare& comp)
{
  typedef typename KeyFromValue::result_type key_type;

  return ranked_index_lower_bound_rank(
    top,y,key,x,comp,
    promotes_2nd_arg<CompatibleCompare,key_type,CompatibleKey>());
}

template<
  typename Node,typename KeyFromValue,
  typename CompatibleCompare
>
inline std::size_t ranked_index_lower_bound_rank(
  Node* top,Node* y,const KeyFromValue& key,
  const BOOST_DEDUCED_TYPENAME KeyFromValue::result_type& x,
  const CompatibleCompare& comp,mpl::true_)
{
  return ranked_index_lower_bound_rank(top,y,key,x,comp,mpl::false_());
}

template<
  typename Node,typename KeyFromValue,
  typename CompatibleKey,typename CompatibleCompare
>
inline std::size_t ranked_index_lower_bound_rank(
  Node* top,Node* y,const KeyFromValue& key,const CompatibleKey& x,
  const CompatibleCompare& comp,mpl::false_)
{
  if(!top)return 0;

  std::size_t s=top->impl()->size;

  do{
    if(!comp(key(top->value()),x)){
      y=top;
      s-=ranked_node_size(y->right())+1;
      top=Node::from_impl(top->left());
    }
    else top=Node::from_impl(top->right());
  }while(top);

  return s;
}

template<
  typename Node,typename KeyFromValue,
  typename CompatibleKey,typename CompatibleCompare
>
inline std::size_t ranked_index_upper_bound_rank(
  Node* top,Node* y,const KeyFromValue& key,const CompatibleKey& x,
  const CompatibleCompare& comp)
{
  typedef typename KeyFromValue::result_type key_type;

  return ranked_index_upper_bound_rank(
    top,y,key,x,comp,
    promotes_1st_arg<CompatibleCompare,CompatibleKey,key_type>());
}

template<
  typename Node,typename KeyFromValue,
  typename CompatibleCompare
>
inline std::size_t ranked_index_upper_bound_rank(
  Node* top,Node* y,const KeyFromValue& key,
  const BOOST_DEDUCED_TYPENAME KeyFromValue::result_type& x,
  const CompatibleCompare& comp,mpl::true_)
{
  return ranked_index_upper_bound_rank(top,y,key,x,comp,mpl::false_());
}

template<
  typename Node,typename KeyFromValue,
  typename CompatibleKey,typename CompatibleCompare
>
inline std::size_t ranked_index_upper_bound_rank(
  Node* top,Node* y,const KeyFromValue& key,const CompatibleKey& x,
  const CompatibleCompare& comp,mpl::false_)
{
  if(!top)return 0;

  std::size_t s=top->impl()->size;

  do{
    if(comp(x,key(top->value()))){
      y=top;
      s-=ranked_node_size(y->right())+1;
      top=Node::from_impl(top->left());
    }
    else top=Node::from_impl(top->right());
  }while(top);

  return s;
}

template<
  typename Node,typename KeyFromValue,
  typename CompatibleKey,typename CompatibleCompare
>
inline std::pair<std::size_t,std::size_t> ranked_index_equal_range_rank(
  Node* top,Node* y,const KeyFromValue& key,const CompatibleKey& x,
  const CompatibleCompare& comp)
{
  typedef typename KeyFromValue::result_type key_type;

  return ranked_index_equal_range_rank(
    top,y,key,x,comp,
    mpl::and_<
      promotes_1st_arg<CompatibleCompare,CompatibleKey,key_type>,
      promotes_2nd_arg<CompatibleCompare,key_type,CompatibleKey> >());
}

template<
  typename Node,typename KeyFromValue,
  typename CompatibleCompare
>
inline std::pair<std::size_t,std::size_t> ranked_index_equal_range_rank(
  Node* top,Node* y,const KeyFromValue& key,
  const BOOST_DEDUCED_TYPENAME KeyFromValue::result_type& x,
  const CompatibleCompare& comp,mpl::true_)
{
  return ranked_index_equal_range_rank(top,y,key,x,comp,mpl::false_());
}

template<
  typename Node,typename KeyFromValue,
  typename CompatibleKey,typename CompatibleCompare
>
inline std::pair<std::size_t,std::size_t> ranked_index_equal_range_rank(
  Node* top,Node* y,const KeyFromValue& key,const CompatibleKey& x,
  const CompatibleCompare& comp,mpl::false_)
{
  if(!top)return std::pair<std::size_t,std::size_t>(0,0);

  std::size_t s=top->impl()->size;

  do{
    if(comp(key(top->value()),x)){
      top=Node::from_impl(top->right());
    }
    else if(comp(x,key(top->value()))){
      y=top;
      s-=ranked_node_size(y->right())+1;
      top=Node::from_impl(top->left());
    }
    else{
      return std::pair<std::size_t,std::size_t>(
        s-top->impl()->size+
          ranked_index_lower_bound_rank(
           Node::from_impl(top->left()),top,key,x,comp,mpl::false_()),
        s-ranked_node_size(top->right())+
          ranked_index_upper_bound_rank(
            Node::from_impl(top->right()),y,key,x,comp,mpl::false_()));
    }
  }while(top);

  return std::pair<std::size_t,std::size_t>(s,s);
}

} /* namespace multi_index::detail */

} /* namespace multi_index */

} /* namespace boost */

#endif
