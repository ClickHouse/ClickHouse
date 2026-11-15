---
title: Consignes relatives aux entrées du changelog
---

<div id="changelog-entry-guidelines">
  # Consignes pour les entrées de changelog
</div>

De bonnes entrées de changelog aident les utilisateurs à comprendre rapidement les nouveautés et leur impact. Nous demandons aux contributeurs de rédiger une entrée du changelog compréhensible par les utilisateurs, qui sera incluse dans le changelog de chaque version.

Ci-dessous, nous présentons quelques règles simples pour rédiger une bonne entrée du changelog.

<div id="write-with-the-user-in-mind-not-the-developer">
  ## Écrivez en pensant à l’utilisateur, pas au développeur
</div>

L’entrée du changelog vise à communiquer le changement à l’*utilisateur*, et pas seulement au *développeur*.
Lorsque vous rédigez l’entrée, essayez, lorsque c’est pertinent, d’expliquer non seulement ***ce qui*** a changé, mais aussi ***pourquoi*** ce changement est utile à l’utilisateur, ou ***comment*** il l’affecte.

Par exemple, au lieu de :

> Adds `system.iceberg_history` table

Écrivez :

> Les utilisateurs peuvent désormais consulter l’historique des snapshots des tables Iceberg grâce à la nouvelle table `system.iceberg_history`.

Au lieu de :

> Add `stringBytesUniq` and `stringBytesEntropy` functions to search for possibly random or encrypted data.&quot;

Écrivez :

> Vous pouvez désormais détecter dans vos chaînes des données potentiellement chiffrées ou aléatoires à l’aide des nouvelles fonctions `stringBytesUniq` et `stringBytesEntropy`, ce qui permet d’identifier des problèmes de qualité des données ou des risques de sécurité.

<div id="keep-it-simple">
  ## Faites simple
</div>

Évitez le jargon technique qu’un utilisateur pourrait ne pas comprendre sans explication. Visez 1 à 5 phrases, et
n’hésitez pas à utiliser un LLM pour vous aider à repérer les fautes de frappe, les erreurs de grammaire ou à reformuler l’entrée de façon
claire et accessible (ce n’est pas de la triche, je vous le promets !)

Au lieu de :

> Prise en charge des sous-requêtes corrélées comme argument de l’expression `EXISTS`

Écrivez :

> Vous pouvez désormais utiliser des sous-requêtes qui référencent des colonnes de la requête externe dans les clauses `EXISTS`.

Un exemple d’entrée claire et simple :

> Permet d’ajuster les paramètres du page cache au niveau de chaque requête. C’est utile pour expérimenter plus rapidement et pour affiner le réglage des requêtes à haut débit et à faible latence.

<div id="follow-a-few-simple-formatting-guidelines">
  ## Suivez quelques règles simples de mise en forme
</div>

<div id="write-in-full-sentences-and-in-the-present-tense">
  ### Écrivez des phrases complètes au présent
</div>

Au lieu de :

> Correction d’un crash : si une exception est levée lors de la tentative de suppression d’un fichier temporaire

Écrivez :

> Corrige un crash lorsqu’une exception est levée lors de la tentative de suppression d’un fichier temporaire.

<div id="use-backticks-where-necessary">
  ### Utilisez des backticks lorsque nécessaire
</div>

Mettez entre backticks les éléments de code tels que les settings, les noms de fonctions, les statements SQL, les noms de format et les data types. De manière générale,
tout ce que vous taperiez dans `clickhouse-client` doit être mis entre backticks. Cela rend les entrées du changelog plus lisibles.

Au lieu de :

> Les settings use&#95;skip&#95;indexes&#95;if&#95;final et use&#95;skip&#95;indexes&#95;if&#95;final&#95;exact&#95;mode sont désormais définis par défaut sur True

Écrivez :

> Les settings `use_skip_indexes_if_final` et `use_skip_indexes_if_final_exact_mode` sont désormais définis par défaut sur `True`

<div id="try-to-follow-a-consistent-format">
  ### Essayez de suivre un format cohérent
</div>

Essayez d’adopter le même format : ce que cela fait → pourquoi c’est important pour l’utilisateur → comment l’utiliser (si nécessaire). Les entrées sont ainsi plus faciles à parcourir et plus prévisibles pour les lecteurs.

Par exemple :

> Vous pouvez désormais filtrer les résultats de recherche vectorielle avant ou après l’opération de recherche, ce qui vous permet de mieux maîtriser le compromis entre performances et précision. Utilisez le nouveau paramètre `vector_search_filter_mode` pour choisir l’approche qui vous convient.