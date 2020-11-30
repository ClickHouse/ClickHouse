---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 62
toc_title: Vue d'ensemble de L'Architecture ClickHouse
---

# Vue d'ensemble de L'Architecture ClickHouse {#overview-of-clickhouse-architecture}

ClickHouse est un véritable SGBD orienté colonne. Les données sont stockées par colonnes et lors de l'exécution de tableaux (vecteurs ou morceaux de colonnes). Dans la mesure du possible, les opérations sont distribuées sur des tableaux, plutôt que sur des valeurs individuelles. Il est appelé “vectorized query execution,” et cela aide à réduire le coût du traitement des données réel.

> Cette idée n'est pas nouvelle. Il remonte à la `APL` langage de programmation et ses descendants: `A +`, `J`, `K`, et `Q`. La programmation de tableau est utilisée dans le traitement des données scientifiques. Cette idée n'est pas non plus nouvelle dans les bases de données relationnelles: par exemple, elle est utilisée dans le `Vectorwise` système.

Il existe deux approches différentes pour accélérer le traitement des requêtes: l'exécution vectorisée des requêtes et la génération de code d'exécution. Ce dernier supprime toute indirection et expédition dynamique. Aucune de ces approches est strictement meilleure que l'autre. La génération de code d'exécution peut être meilleure lorsqu'elle fusionne de nombreuses opérations, utilisant ainsi pleinement les unités D'exécution du processeur et le pipeline. L'exécution de requête vectorisée peut être moins pratique car elle implique des vecteurs temporaires qui doivent être écrits dans le cache et lus. Si les données temporaires ne rentre pas dans le cache L2, cela devient un problème. Mais l'exécution de requête vectorisée utilise plus facilement les capacités SIMD de la CPU. Un [document de recherche](http://15721.courses.cs.cmu.edu/spring2016/papers/p5-sompolski.pdf) écrit par nos amis montre qu'il est préférable de combiner les deux approches. ClickHouse utilise l'exécution de requête vectorisée et a un support initial limité pour la génération de code d'exécution.

## Colonne {#columns}

`IColumn` l'interface est utilisée pour représenter des colonnes en mémoire (en fait, des morceaux de colonnes). Cette interface fournit des méthodes d'aide pour la mise en œuvre de divers opérateurs relationnels. Presque toutes les opérations sont immuables: elles ne modifient pas la colonne d'origine, mais en créent une nouvelle modifiée. Par exemple, l' `IColumn :: filter` méthode accepte un masque d'octet de filtre. Il est utilisé pour le `WHERE` et `HAVING` opérateurs relationnels. Exemples supplémentaires: `IColumn :: permute` méthode de soutien `ORDER BY`, le `IColumn :: cut` méthode de soutien `LIMIT`.

Divers `IColumn` application (`ColumnUInt8`, `ColumnString` et ainsi de suite) sont responsables de la mémoire disposition de colonnes. La disposition de la mémoire est généralement un tableau contigu. Pour le type entier de colonnes, c'est juste un contiguë tableau, comme `std :: vector`. Pour `String` et `Array` colonnes, il s'agit de deux vecteurs: Un pour tous les éléments du tableau, placé de manière contiguë, et un second pour les décalages au début de chaque tableau. Il y a aussi `ColumnConst` cela stocke une seule valeur en mémoire, mais ressemble à une colonne.

## Champ {#field}

Néanmoins, il est possible de travailler avec des valeurs individuelles ainsi. Pour représenter une valeur individuelle, la `Field` est utilisée. `Field` est juste une union discriminée de `UInt64`, `Int64`, `Float64`, `String` et `Array`. `IColumn` a l' `operator[]` méthode pour obtenir la n-ème valeur en tant que `Field` et la `insert` méthode pour ajouter un `Field` à la fin d'une colonne. Ces méthodes ne sont pas très efficaces, car ils nécessitent de traiter avec temporaire `Field` des objets représentant une valeur individuelle. Il existe des méthodes plus efficaces, telles que `insertFrom`, `insertRangeFrom` et ainsi de suite.

`Field` ne pas avoir assez d'informations sur un type de données spécifique pour une table. Exemple, `UInt8`, `UInt16`, `UInt32`, et `UInt64` tous sont représentés comme `UInt64` dans un `Field`.

## Abstractions Qui Fuient {#leaky-abstractions}

`IColumn` a des méthodes pour les transformations relationnelles communes des données, mais elles ne répondent pas à tous les besoins. Exemple, `ColumnUInt64` ne pas avoir une méthode pour calculer la somme des deux colonnes, et `ColumnString` n'a pas de méthode pour exécuter une recherche de sous-chaîne. Ces innombrables routines sont mises en œuvre en dehors de `IColumn`.

Diverses fonctions sur les colonnes peuvent être implémentées de manière générique et non efficace en utilisant `IColumn` méthodes pour extraire `Field` valeurs, ou d'une manière spécialisée en utilisant la connaissance de la disposition de la mémoire interne des données dans un `IColumn` application. Il est implémenté en lançant des fonctions à un `IColumn` tapez et traitez directement la représentation interne. Exemple, `ColumnUInt64` a l' `getData` méthode qui renvoie une référence à un tableau interne, puis une autre routine lit ou remplit ce tableau directement. Nous avons “leaky abstractions” permettent de spécialisations diverses routines.

## Types De Données {#data_types}

`IDataType` est responsable de la sérialisation et de la désérialisation: pour la lecture et l'écriture de morceaux de colonnes ou de valeurs individuelles sous forme binaire ou de texte. `IDataType` correspond directement aux types de données dans les tables. Par exemple, il y a `DataTypeUInt32`, `DataTypeDateTime`, `DataTypeString` et ainsi de suite.

`IDataType` et `IColumn` ne sont que faiblement liés les uns aux autres. Différents types de données peuvent être représentés en mémoire par le même `IColumn` application. Exemple, `DataTypeUInt32` et `DataTypeDateTime` sont tous deux représentés par `ColumnUInt32` ou `ColumnConstUInt32`. En outre, le même type de données peut être représentée par différents `IColumn` application. Exemple, `DataTypeUInt8` peut être représenté par `ColumnUInt8` ou `ColumnConstUInt8`.

`IDataType` stocke uniquement les métadonnées. Par exemple, `DataTypeUInt8` ne stocke rien du tout (sauf vptr) et `DataTypeFixedString` magasins juste `N` (la taille des chaînes de taille fixe).

`IDataType` a des méthodes d'aide pour différents formats de données. Des exemples sont des méthodes pour sérialiser une valeur avec des guillemets possibles, pour sérialiser une valeur pour JSON et pour sérialiser une valeur dans le format XML. Il n'y a pas de correspondance directe avec les formats de données. Par exemple, les différents formats de données `Pretty` et `TabSeparated` pouvez utiliser le même `serializeTextEscaped` méthode d'aide à partir de la `IDataType` interface.

## Bloc {#block}

A `Block` est un conteneur qui représente un sous-ensemble (morceau) d'une table en mémoire. C'est juste un ensemble de triplets: `(IColumn, IDataType, column name)`. Pendant l'exécution de la requête, les données sont traitées par `Block`s. Si nous avons un `Block`, nous disposons de données (dans le `IColumn` objet), nous avons des informations sur son type (dans `IDataType`) qui nous indique comment traiter cette colonne, et nous avons le nom de la colonne. Il peut s'agir du nom de colonne d'origine de la table ou d'un nom artificiel attribué pour obtenir des résultats temporaires de calculs.

Lorsque nous calculons une fonction sur des colonnes dans un bloc, nous ajoutons une autre colonne avec son résultat au bloc, et nous ne touchons pas les colonnes pour les arguments de la fonction car les opérations sont immuables. Plus tard, les colonnes inutiles peuvent être supprimées du bloc, mais pas modifiées. Il est pratique pour l'élimination des sous-expressions communes.

Des blocs sont créés pour chaque bloc de données traité. Notez que pour le même type de calcul, les noms et les types de colonnes restent les mêmes pour différents blocs, et seules les données de colonne changent. Il est préférable de diviser les données de bloc de l'en-tête de bloc car les petites tailles de Bloc ont une surcharge élevée de chaînes temporaires pour copier shared\_ptrs et les noms de colonnes.

## Bloquer Les Flux {#block-streams}

Les flux de blocs sont destinés au traitement des données. Nous utilisons des flux de blocs pour lire des données quelque part, effectuer des transformations de données ou écrire des données quelque part. `IBlockInputStream` a l' `read` méthode pour récupérer le bloc suivant, tandis que des. `IBlockOutputStream` a l' `write` méthode pour pousser le bloc quelque part.

Les flux sont responsables de:

1.  De la lecture ou de l'écriture dans une table. La table renvoie simplement un flux pour lire ou écrire des blocs.
2.  Mise en œuvre des formats de données. Par exemple, si vous souhaitez envoyer des données vers un terminal `Pretty` format, vous créez un flux de sortie de bloc où vous poussez des blocs, et il les formate.
3.  Effectuer des transformations de données. Disons que vous avez `IBlockInputStream` et veulent créer un flux filtré. Vous créez `FilterBlockInputStream` et l'initialiser avec votre flux de données. Puis quand vous tirez un bloc de `FilterBlockInputStream`, il extrait un bloc de votre flux, le filtre et vous renvoie le bloc filtré. Les pipelines d'exécution des requêtes sont représentés de cette façon.

Il y a des transformations plus sophistiquées. Par exemple, lorsque vous tirez de `AggregatingBlockInputStream` il lit toutes les données à partir de sa source, agrégats, puis renvoie un flux de données agrégées pour vous. Un autre exemple: `UnionBlockInputStream` accepte de nombreuses sources d'entrée dans le constructeur et également un certain nombre de threads. Il lance plusieurs threads et lit à partir de plusieurs sources en parallèle.

> Les flux de blocs utilisent le “pull” approche pour contrôler le flux: lorsque vous extrayez un bloc du premier flux, il extrait par conséquent les blocs requis des flux imbriqués, et l'ensemble du pipeline d'exécution fonctionnera. Ni “pull” ni “push” est la meilleure solution, car le flux de contrôle est implicite, ce qui limite l'implémentation de diverses fonctionnalités telles que l'exécution simultanée de plusieurs requêtes (fusion de plusieurs pipelines ensemble). Cette limitation pourrait être surmontée avec des coroutines ou simplement en exécutant des threads supplémentaires qui s'attendent les uns aux autres. Nous pouvons avoir plus de possibilités si nous rendons le flux de contrôle explicite: si nous localisons la logique pour passer des données d'une unité de calcul à une autre en dehors de ces unités de calcul. Lire ce [article](http://journal.stuffwithstuff.com/2013/01/13/iteration-inside-and-out/) pour plus de pensées.

Il convient de noter que le pipeline d'exécution de la requête crée des données temporaires à chaque étape. Nous essayons de garder la taille du bloc suffisamment petite pour que les données temporaires tiennent dans le cache du processeur. Avec cette hypothèse, l'écriture et la lecture de données temporaires sont presque libres en comparaison avec d'autres calculs. Nous pourrions envisager une alternative, qui est de fusionner de nombreuses opérations dans le pipeline ensemble. Cela pourrait rendre le pipeline aussi court que possible et supprimer une grande partie des données temporaires, ce qui pourrait être un avantage, mais cela présente également des inconvénients. Par exemple, un pipeline divisé facilite l'implémentation de la mise en cache de données intermédiaires, le vol de données intermédiaires à partir de requêtes similaires exécutées en même temps et la fusion de pipelines pour des requêtes similaires.

## Format {#formats}

Les formats de données sont implémentés avec des flux de blocs. Il y a “presentational” formats appropriés uniquement pour la sortie de données vers le client, tels que `Pretty` format, qui fournit seulement `IBlockOutputStream`. Et il existe des formats d'entrée / sortie, tels que `TabSeparated` ou `JSONEachRow`.

Il y a aussi des flux de lignes: `IRowInputStream` et `IRowOutputStream`. Ils vous permettent de tirer/pousser des données par des lignes individuelles, pas par des blocs. Et ils ne sont nécessaires que pour simplifier la mise en œuvre des formats orientés ligne. Wrapper `BlockInputStreamFromRowInputStream` et `BlockOutputStreamFromRowOutputStream` vous permet de convertir des flux orientés ligne en flux orientés blocs réguliers.

## I/O {#io}

Pour l'entrée/sortie orientée octet, il y a `ReadBuffer` et `WriteBuffer` les classes abstraites. Ils sont utilisés à la place de C++ `iostream`s. Ne vous inquiétez pas: chaque projet c++ mature utilise autre chose que `iostream`s pour de bonnes raisons.

`ReadBuffer` et `WriteBuffer` sont juste un tampon contigu et un curseur pointant vers la position dans ce tampon. Les implémentations peuvent posséder ou non la mémoire du tampon. Il existe une méthode virtuelle pour remplir le tampon avec les données suivantes (pour `ReadBuffer`) ou pour vider le tampon quelque part (pour `WriteBuffer`). Les méthodes virtuelles sont rarement cités.

Les implémentations de `ReadBuffer`/`WriteBuffer` sont utilisés pour travailler avec des fichiers et des descripteurs de fichiers et des sockets réseau, pour implémenter la compression (`CompressedWriteBuffer` is initialized with another WriteBuffer and performs compression before writing data to it), and for other purposes – the names `ConcatReadBuffer`, `LimitReadBuffer`, et `HashingWriteBuffer` parler pour eux-mêmes.

Read / WriteBuffers ne traite que les octets. Il y a des fonctions de `ReadHelpers` et `WriteHelpers` fichiers d'en-tête pour aider à formater l'entrée / sortie. Par exemple, il existe des assistants pour écrire un nombre au format décimal.

Regardons ce qui se passe lorsque vous voulez écrire un ensemble de résultats dans `JSON` format de sortie standard (stdout). Vous avez un jeu de résultats prêt à être récupéré `IBlockInputStream`. Vous créez `WriteBufferFromFileDescriptor(STDOUT_FILENO)` pour écrire des octets dans stdout. Vous créez `JSONRowOutputStream`, initialisé avec qui `WriteBuffer`, pour écrire des lignes dans `JSON` à stdout. Vous créez `BlockOutputStreamFromRowOutputStream` de plus, pour la représenter comme `IBlockOutputStream`. Ensuite, vous appelez `copyData` pour transférer des données de `IBlockInputStream` de `IBlockOutputStream` et tout fonctionne. Interne, `JSONRowOutputStream` écrira divers délimiteurs JSON et appellera `IDataType::serializeTextJSON` méthode avec une référence à `IColumn` et le numéro de ligne comme arguments. Conséquent, `IDataType::serializeTextJSON` appellera une méthode de `WriteHelpers.h`: exemple, `writeText` pour les types numériques et `writeJSONString` pour `DataTypeString`.

## Table {#tables}

Le `IStorage` l'interface représente les tables. Différentes implémentations de cette interface sont des moteurs de table différents. Les exemples sont `StorageMergeTree`, `StorageMemory` et ainsi de suite. Les Instances de ces classes ne sont que des tables.

Clé `IStorage` les méthodes sont `read` et `write`. Il y a aussi des `alter`, `rename`, `drop` et ainsi de suite. Le `read` méthode accepte les arguments suivants: l'ensemble de colonnes à lire à partir d'un tableau, l' `AST` requête à considérer, et le nombre souhaité de flux de retour. Il renvoie un ou plusieurs `IBlockInputStream` objets et informations sur l'étape de traitement des données qui a été effectuée dans un moteur de table lors de l'exécution de la requête.

Dans la plupart des cas, la méthode read n'est responsable que de la lecture des colonnes spécifiées à partir d'une table, et non d'un traitement ultérieur des données. Tout traitement ultérieur des données est effectué par l'interpréteur de requêtes et n'est pas de la responsabilité de `IStorage`.

Mais il y a des exceptions notables:

-   La requête AST est transmise au `read` et le moteur de table peut l'utiliser pour dériver l'utilisation de l'index et pour lire moins de données à partir d'une table.
-   Parfois, le moteur de table peut traiter les données lui-même à une étape spécifique. Exemple, `StorageDistributed` peut envoyer une requête aux serveurs distants, leur demander de traiter les données à une étape où les données de différents serveurs distants peuvent être fusionnées, et renvoyer ces données prétraitées. L'interpréteur de requête termine ensuite le traitement des données.

Table `read` la méthode peut retourner plusieurs `IBlockInputStream` objets permettant le traitement parallèle des données. Ces flux d'entrée de bloc multiples peuvent lire à partir d'une table en parallèle. Ensuite, vous pouvez envelopper ces flux avec diverses transformations (telles que l'évaluation d'expression ou le filtrage) qui peuvent être calculées indépendamment et créer un `UnionBlockInputStream` en plus d'eux, pour lire à partir de plusieurs flux en parallèle.

Il y a aussi des `TableFunction`s. Ce sont des fonctions qui renvoient un `IStorage` objet à utiliser dans le `FROM` la clause d'une requête.

Pour avoir une idée rapide de la façon d'implémenter votre moteur de table, regardez quelque chose de simple, comme `StorageMemory` ou `StorageTinyLog`.

> Comme le résultat de l' `read` méthode, `IStorage` retourner `QueryProcessingStage` – information about what parts of the query were already calculated inside storage.

## Analyseur {#parsers}

Un analyseur de descente récursif écrit à la main analyse une requête. Exemple, `ParserSelectQuery` appelle simplement récursivement les analyseurs sous-jacents pour diverses parties de la requête. Les analyseurs créent un `AST`. Le `AST` est représenté par des nœuds, qui sont des instances de `IAST`.

> Les générateurs d'analyseurs ne sont pas utilisés pour des raisons historiques.

## Interprète {#interpreters}

Les interprètes sont responsables de la création du pipeline d'exécution des requêtes à partir `AST`. Il existe des interprètes simples, tels que `InterpreterExistsQuery` et `InterpreterDropQuery` ou le plus sophistiqué de `InterpreterSelectQuery`. Le pipeline d'exécution de requête est une combinaison de flux d'entrée ou de sortie de bloc. Par exemple, le résultat de l'interprétation de la `SELECT` la requête est la `IBlockInputStream` pour lire le jeu de résultats; le résultat de la requête d'INSERTION est l' `IBlockOutputStream` pour écrire des données à insérer, et le résultat de l'interprétation `INSERT SELECT` la requête est la `IBlockInputStream` cela renvoie un jeu de résultats vide lors de la première lecture, mais qui copie `SELECT` de `INSERT` dans le même temps.

`InterpreterSelectQuery` utiliser `ExpressionAnalyzer` et `ExpressionActions` machines pour l'analyse des requêtes et des transformations. C'est là que la plupart des optimisations de requêtes basées sur des règles sont effectuées. `ExpressionAnalyzer` est assez désordonné et devrait être réécrit: diverses transformations et optimisations de requête doivent être extraites dans des classes séparées pour permettre des transformations modulaires ou une requête.

## Fonction {#functions}

Il y a des fonctions ordinaires et des fonctions agrégées. Pour les fonctions d'agrégation, voir la section suivante.

Ordinary functions don't change the number of rows – they work as if they are processing each row independently. In fact, functions are not called for individual rows, but for `Block`'s de données pour implémenter l'exécution de requête vectorisée.

Il y a quelques fonctions diverses, comme [la taille de bloc](../sql-reference/functions/other-functions.md#function-blocksize), [rowNumberInBlock](../sql-reference/functions/other-functions.md#function-rownumberinblock), et [runningAccumulate](../sql-reference/functions/other-functions.md#function-runningaccumulate), qui exploitent le traitement de bloc et violent l'indépendance des lignes.

ClickHouse a un typage fort, donc il n'y a pas de conversion de type implicite. Si une fonction ne prend pas en charge une combinaison spécifique de types, elle lève une exception. Mais les fonctions peuvent fonctionner (être surchargées) pour de nombreuses combinaisons de types différentes. Par exemple, l' `plus` fonction (pour mettre en œuvre la `+` opérateur) fonctionne pour toute combinaison de types numériques: `UInt8` + `Float32`, `UInt16` + `Int8` et ainsi de suite. En outre, certaines fonctions variadiques peuvent accepter n'importe quel nombre d'arguments, tels que `concat` fonction.

L'implémentation d'une fonction peut être légèrement gênante car une fonction distribue explicitement les types de données pris en charge et pris en charge `IColumns`. Par exemple, l' `plus` la fonction a du code généré par l'instanciation D'un modèle C++ pour chaque combinaison de types numériques, et des arguments gauche et droit constants ou non constants.

C'est un excellent endroit pour implémenter la génération de code d'exécution pour éviter le gonflement du code de modèle. En outre, il permet d'ajouter des fonctions fusionnées comme Fusionné Multiplier-Ajouter ou de faire plusieurs comparaisons dans une itération de boucle.

En raison de l'exécution de requête vectorisée, les fonctions ne sont pas court-circuitées. Par exemple, si vous écrivez `WHERE f(x) AND g(y)` les deux faces sont calculés, même pour les lignes, quand `f(x)` est égal à zéro (sauf quand `f(x)` est une expression constante nulle). Mais si la sélectivité de l' `f(x)` la condition est élevée, et le calcul de `f(x)` est beaucoup moins cher que `g(y)`, il est préférable d'implémenter le calcul multi-pass. Il serait d'abord calculer `f(x)` puis filtrer les colonnes par la suite, puis de calculer `g(y)` uniquement pour les petits morceaux de données filtrés.

## Les Fonctions D'Agrégation {#aggregate-functions}

Les fonctions d'agrégation sont des fonctions avec État. Ils accumulent les valeurs passées dans certains etats et vous permettent d'obtenir des résultats de cet état. Ils sont gérés avec le `IAggregateFunction` interface. Les États peuvent être assez simples (l'État pour `AggregateFunctionCount` est juste un seul `UInt64` valeur) ou très complexes (l'état de `AggregateFunctionUniqCombined` est une combinaison linéaire du tableau, d'une table de hachage, et un `HyperLogLog` structure probabiliste des données).

Les États sont répartis en `Arena` (un pool de mémoire) pour traiter plusieurs états lors de l'exécution d'une cardinalité élevée `GROUP BY` requête. Les États peuvent avoir un constructeur et un destructeur non triviaux: par exemple, les États d'agrégation compliqués peuvent allouer eux-mêmes de la mémoire supplémentaire. Il faut accorder une certaine attention à la création et à la destruction des États et à la transmission appropriée de leur propriété et de leur ordre de destruction.

Les États d'agrégation peuvent être sérialisés et désérialisés pour passer sur le réseau pendant l'exécution de la requête distribuée ou pour les écrire sur le disque où il n'y a pas assez de RAM. Ils peuvent même être stockés dans une table avec le `DataTypeAggregateFunction` pour permettre l'agrégation incrémentielle des données.

> Le format de données sérialisé pour les états de fonction d'agrégat n'est pas versionné pour le moment. C'est ok si les États d'agrégat ne sont stockés que temporairement. Mais nous avons l' `AggregatingMergeTree` moteur de table pour l'agrégation incrémentielle, et les gens l'utilisent déjà en production. C'est la raison pour laquelle la rétrocompatibilité est requise lors de la modification du format sérialisé pour toute fonction d'agrégat à l'avenir.

## Serveur {#server}

Le serveur implémente plusieurs interfaces différentes:

-   Une interface HTTP pour tous les clients étrangers.
-   Une interface TCP pour le client clickhouse natif et pour la communication inter-serveur lors de l'exécution de la requête distribuée.
-   Une interface pour transférer des données pour la réplication.

En interne, il s'agit simplement d'un serveur multithread primitif sans coroutines ni fibres. Étant donné que le serveur n'est pas conçu pour traiter un taux élevé de requêtes simples, mais pour traiter un taux relativement faible de requêtes complexes, chacun d'eux peut traiter une grande quantité de données à des fins d'analyse.

Le serveur initialise le `Context` classe avec l'environnement nécessaire à l'exécution des requêtes: la liste des bases de données disponibles, des utilisateurs et des droits d'accès, des paramètres, des clusters, la liste des processus, le journal des requêtes, etc. Les interprètes utilisent cet environnement.

Nous maintenons une compatibilité ascendante et descendante complète pour le protocole TCP du serveur: les anciens clients peuvent parler à de nouveaux serveurs, et les nouveaux clients peuvent parler à d'anciens serveurs. Mais nous ne voulons pas le maintenir éternellement, et nous supprimons le support pour les anciennes versions après environ un an.

!!! note "Note"
    Pour la plupart des applications externes, nous vous recommandons d'utiliser L'interface HTTP car elle est simple et facile à utiliser. Le protocole TCP est plus étroitement lié aux structures de données internes: il utilise un format interne pour passer des blocs de données, et il utilise un cadrage personnalisé pour les données compressées. Nous n'avons pas publié de bibliothèque C pour ce protocole car elle nécessite de lier la plupart de la base de code ClickHouse, ce qui n'est pas pratique.

## Exécution De Requête Distribuée {#distributed-query-execution}

Les serveurs d'une configuration de cluster sont pour la plupart indépendants. Vous pouvez créer un `Distributed` table sur un ou tous les serveurs dans un cluster. Le `Distributed` table does not store data itself – it only provides a “view” à toutes les tables sur plusieurs nœuds d'un cluster. Lorsque vous sélectionnez à partir d'un `Distributed` table, il réécrit cette requête, choisit les nœuds distants en fonction des paramètres d'équilibrage de charge et leur envoie la requête. Le `Distributed` table demande aux serveurs distants de traiter une requête jusqu'à une étape où les résultats intermédiaires de différents serveurs peuvent être fusionnés. Puis il reçoit les résultats intermédiaires et les fusionne. La table distribuée essaie de distribuer autant de travail que possible aux serveurs distants et n'envoie pas beaucoup de données intermédiaires sur le réseau.

Les choses deviennent plus compliquées lorsque vous avez des sous-requêtes dans des clauses IN ou JOIN, et que chacune d'elles utilise un `Distributed` table. Nous avons différentes stratégies pour l'exécution de ces requêtes.

Il n'existe pas de plan de requête global pour l'exécution des requêtes distribuées. Chaque nœud a son plan de requête local pour sa partie du travail. Nous n'avons qu'une simple exécution de requête distribuée en une seule passe: nous envoyons des requêtes pour les nœuds distants, puis fusionnons les résultats. Mais cela n'est pas possible pour les requêtes compliquées avec des groupes de cardinalité élevés ou avec une grande quantité de données temporaires pour la jointure. Dans de tels cas, nous avons besoin de “reshuffle” données entre les serveurs, ce qui nécessite une coordination supplémentaire. ClickHouse ne supporte pas ce type d'exécution de requête, et nous devons y travailler.

## Fusion De L'Arbre {#merge-tree}

`MergeTree` est une famille de moteurs de stockage qui prend en charge l'indexation par clé primaire. La clé primaire peut être un tuple arbitraire de colonnes ou d'expressions. De données dans un `MergeTree` la table est stockée dans “parts”. Chaque partie stocke les données dans l'ordre de la clé primaire, de sorte que les données sont ordonnées lexicographiquement par le tuple de clé primaire. Toutes les colonnes du tableau sont stockés dans différents `column.bin` les fichiers dans ces régions. Les fichiers sont constitués de blocs compressés. Chaque bloc est généralement de 64 KO à 1 Mo de données non compressées, en fonction de la taille de la valeur moyenne. Les blocs sont constitués de valeurs de colonne placées de manière contiguë l'une après l'autre. Les valeurs de colonne sont dans le même ordre pour chaque colonne (la clé primaire définit l'ordre), donc lorsque vous itérez par plusieurs colonnes, vous obtenez des valeurs pour les lignes correspondantes.

La clé primaire elle-même est “sparse”. Il ne traite pas chaque ligne, mais seulement certaines plages de données. Séparé `primary.idx` fichier a la valeur de la clé primaire pour chaque N-ième ligne, où N est appelé `index_granularity` (habituellement, N = 8192). Aussi, pour chaque colonne, nous avons `column.mrk` les fichiers avec l' “marks,” qui sont des décalages à chaque N-ème ligne dans le fichier de données. Chaque marque est une paire: le décalage dans le fichier au début du bloc compressé, et le décalage dans le bloc décompressé au début des données. Habituellement, les blocs compressés sont alignés par des marques, et le décalage dans le bloc décompressé est nul. Les données pour `primary.idx` réside toujours dans la mémoire, et les données pour `column.mrk` les fichiers sont mis en cache.

Quand nous allons lire quelque chose d'une partie dans `MergeTree` nous regardons `primary.idx` données et locate plages qui pourraient contenir des données demandées, puis regardez `column.mrk` données et calculer des décalages pour savoir où commencer à lire ces plages. En raison de la rareté, les données excédentaires peuvent être lues. ClickHouse ne convient pas à une charge élevée de requêtes ponctuelles simples, car toute la gamme avec `index_granularity` les lignes doivent être lues pour chaque clé, et le bloc compressé entier doit être décompressé pour chaque colonne. Nous avons rendu l'index clairsemé parce que nous devons être en mesure de maintenir des milliards de lignes par serveur unique sans consommation de mémoire notable pour l'index. De plus, comme la clé primaire est clairsemée, elle n'est pas unique: elle ne peut pas vérifier l'existence de la clé dans la table au moment de l'insertion. Vous pourriez avoir plusieurs lignes avec la même clé dans une table.

Lorsque vous `INSERT` un tas de données dans `MergeTree`, ce groupe est trié par ordre de clé primaire et forme une nouvelle partie. Il existe des threads d'arrière-plan qui sélectionnent périodiquement certaines parties et les fusionnent en une seule partie triée pour maintenir le nombre de parties relativement faible. C'est pourquoi il est appelé `MergeTree`. Bien sûr, la fusion conduit à “write amplification”. Toutes les parties sont immuables: elles sont seulement créées et supprimées, mais pas modifiées. Lorsque SELECT est exécuté, il contient un instantané de la table (un ensemble de parties). Après la Fusion, nous conservons également les anciennes pièces pendant un certain temps pour faciliter une récupération après une défaillance, donc si nous voyons qu'une partie fusionnée est probablement cassée, nous pouvons la remplacer par ses parties sources.

`MergeTree` n'est pas un arbre LSM car il ne contient pas “memtable” et “log”: inserted data is written directly to the filesystem. This makes it suitable only to INSERT data in batches, not by individual row and not very frequently – about once per second is ok, but a thousand times a second is not. We did it this way for simplicity's sake, and because we are already inserting data in batches in our applications.

> Les tables MergeTree ne peuvent avoir qu'un seul index (primaire): il n'y a pas d'index secondaires. Il serait bon d'autoriser plusieurs représentations physiques sous une table logique, par exemple, pour stocker des données dans plus d'un ordre physique ou même pour autoriser des représentations avec des données pré-agrégées avec des données originales.

Il existe des moteurs MergeTree qui effectuent un travail supplémentaire lors des fusions en arrière-plan. Les exemples sont `CollapsingMergeTree` et `AggregatingMergeTree`. Cela pourrait être traité comme un support spécial pour les mises à jour. Gardez à l'esprit que ce ne sont pas de vraies mises à jour car les utilisateurs n'ont généralement aucun contrôle sur le moment où les fusions en arrière-plan sont exécutées et les données dans un `MergeTree` la table est presque toujours stockée dans plus d'une partie, pas sous une forme complètement fusionnée.

## Réplication {#replication}

La réplication dans ClickHouse peut être configurée sur une base par table. Vous pouvez avoir des tables répliquées et des tables non répliquées sur le même serveur. Vous pouvez également avoir des tables répliquées de différentes manières, comme une table avec une réplication à deux facteurs et une autre avec trois facteurs.

La réplication est implémentée dans le `ReplicatedMergeTree` moteur de stockage. Le chemin d'accès dans `ZooKeeper` est spécifié comme paramètre pour le moteur de stockage. Toutes les tables avec le même chemin dans `ZooKeeper` devenez des répliques les unes des autres: elles synchronisent leurs données et maintiennent la cohérence. Les répliques peuvent être ajoutées et supprimées dynamiquement simplement en créant ou en supprimant une table.

La réplication utilise un schéma multi-maître asynchrone. Vous pouvez insérer des données dans n'importe quel réplica qui a une session avec `ZooKeeper`, et les données sont répliquées à toutes les autres répliques de manière asynchrone. Parce que ClickHouse ne prend pas en charge les mises à jour, la réplication est sans conflit. Comme il n'y a pas d'accusé de réception de quorum des insertions, les données juste insérées peuvent être perdues si un nœud échoue.

Les métadonnées pour la réplication sont stockées dans ZooKeeper. Il existe un journal de réplication qui répertorie les actions à effectuer. Les Actions sont: obtenir une partie; fusionner des parties; déposer une partition, et ainsi de suite. Chaque réplica copie le journal de réplication dans sa file d'attente, puis exécute les actions de la file d'attente. Par exemple, sur l'insertion, l' “get the part” l'action est créée dans le journal, et chaque réplique téléchargements de la partie. Les fusions sont coordonnées entre les répliques pour obtenir des résultats identiques aux octets. Toutes les parties sont fusionnées de la même manière sur toutes les répliques. Il est réalisé en élisant une réplique en tant que leader, et cette réplique initie fusionne et écrit “merge parts” actions dans le journal.

La réplication est physique: seules les parties compressées sont transférées entre les nœuds, pas les requêtes. Les fusions sont traitées sur chaque réplique indépendamment dans la plupart des cas pour réduire les coûts du réseau en évitant l'amplification du réseau. Grand fusionné les pièces sont envoyées sur le réseau uniquement en cas de retard de réplication.

En outre, chaque réplique stocke son état dans ZooKeeper comme l'ensemble des pièces et ses sommes de contrôle. Lorsque l'état sur le système de fichiers local diverge de l'état de référence dans ZooKeeper, le réplica restaure sa cohérence en téléchargeant les parties manquantes et brisées à partir d'autres réplicas. Lorsqu'il y a des données inattendues ou brisées dans le système de fichiers local, ClickHouse ne les supprime pas, mais les déplace dans un répertoire séparé et les oublie.

!!! note "Note"
    Le cluster ClickHouse est constitué de fragments indépendants, et chaque fragment est constitué de répliques. Le cluster est **pas élastique**, donc, après avoir ajouté un nouveau fragment, les données ne sont pas rééquilibrées automatiquement entre les fragments. Au lieu de cela, la charge du cluster est censée être ajustée pour être inégale. Cette implémentation vous donne plus de contrôle, et c'est ok pour des clusters relativement petits, tels que des dizaines de nœuds. Mais pour les clusters avec des centaines de nœuds que nous utilisons en production, cette approche devient un inconvénient important. Nous devrions implémenter un moteur de table qui s'étend sur le cluster avec des régions répliquées dynamiquement qui pourraient être divisées et équilibrées automatiquement entre les clusters.

{## [Article Original](https://clickhouse.tech/docs/en/development/architecture/) ##}
