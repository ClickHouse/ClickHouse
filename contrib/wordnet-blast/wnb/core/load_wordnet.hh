#pragma once

# include "info_helper.hh"

namespace wnb
{
  /// forward declaration
  struct wordnet;

  /// Load the entire wordnet data base located in \p dn (typically .../dict/)
  void load_wordnet(const std::string& dn, wordnet& wn, info_helper& info);
}
