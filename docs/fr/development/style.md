---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 68
toc_title: "Comment \xE9crire du Code C++ "
---

# Comment écrire du Code C++  {#how-to-write-c-code}

## Recommandations Générales {#general-recommendations}

**1.** Ce qui suit sont des recommandations, pas des exigences.

**2.** Si vous modifiez du code, il est logique de suivre le formatage du code existant.

**3.** Le style de Code est nécessaire pour la cohérence. La cohérence facilite la lecture du code et facilite également la recherche du code.

**4.** Beaucoup de règles n'ont pas de raisons logiques; elles sont dictées par des pratiques établies.

## Formater {#formatting}

**1.** La plupart du formatage se fera automatiquement par `clang-format`.

**2.** Les tirets sont 4 espaces. Configurez votre environnement de développement afin qu'un onglet ajoute quatre espaces.

**3.** Les crochets d'ouverture et de fermeture doivent être sur une ligne séparée.

``` cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** Si le corps entier de la fonction est un `statement` il peut donc être placé sur une seule ligne. Place des espaces autour des accolades (en plus de l'espace à la fin de la ligne).

``` cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** Pour les fonctions. Ne mettez pas d'espaces entre parenthèses.

``` cpp
void reinsert(const Value & x)
```

``` cpp
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** Dans `if`, `for`, `while` et d'autres expressions, un espace est inséré devant le support d'ouverture (par opposition aux appels de fonction).

``` cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** Ajouter des espaces autour des opérateurs binaires (`+`, `-`, `*`, `/`, `%`, …) and the ternary operator `?:`.

``` cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** Si un saut de ligne est entré, placez l'opérateur sur une nouvelle ligne et augmentez le retrait avant.

``` cpp
if (elapsed_ns)
    message << " ("
        << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** Vous pouvez utiliser des espaces pour l'alignement dans une ligne, si vous le souhaitez.

``` cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** N'utilisez pas d'espaces autour des opérateurs `.`, `->`.

Si nécessaire, l'opérateur peut être renvoyé à la ligne suivante. Dans ce cas, le décalage devant celui-ci est augmenté.

**11.** N'utilisez pas d'espace pour séparer les opérateurs unaires (`--`, `++`, `*`, `&`, …) from the argument.

**12.** Mettre un espace après une virgule, mais pas avant. La même règle vaut pour un point-virgule à l'intérieur d'un `for` expression.

**13.** Ne pas utiliser des espaces pour séparer les `[]` opérateur.

**14.** Dans un `template <...>` expression, utiliser un espace entre les `template` et `<`; pas d'espace après `<` ou avant `>`.

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** Dans les classes et les structures, écrivez `public`, `private`, et `protected` sur le même niveau que `class/struct` et tiret le reste du code.

``` cpp
template <typename T>
class MultiVersion
{
public:
    /// Version of object for usage. shared_ptr manage lifetime of version.
    using Version = std::shared_ptr<const T>;
    ...
}
```

**16.** Si le même `namespace` est utilisé pour l'ensemble du fichier, et il n'y a rien d'autre significatif, un décalage n'est pas nécessaire à l'intérieur `namespace`.

**17.** Si le bloc pour un `if`, `for`, `while` ou autres expressions se compose d'un seul `statement`, les accolades sont facultatives. Place de la `statement` sur une ligne séparée, à la place. Cette règle est également valable pour les imbriqués `if`, `for`, `while`, …

Mais si l'intérieur `statement` contient des accolades ou `else` le bloc externe doit être écrit dans les accolades.

``` cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** Il ne devrait pas y avoir d'espaces aux extrémités des lignes.

**19.** Les fichiers Source sont encodés en UTF-8.

**20.** Les caractères non-ASCII peuvent être utilisés dans les littéraux de chaîne.

``` cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** N'écrivez pas plusieurs expressions sur une seule ligne.

**22.** Groupez les sections de code à l'intérieur des fonctions et séparez-les avec pas plus d'une ligne vide.

**23.** Séparez les fonctions, les classes, etc. avec une ou deux lignes vides.

**24.** `A const` (liés à une valeur) doit être écrit avant le nom du type.

``` cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** Lors de la déclaration d'un pointeur ou d'une référence, le `*` et `&` les symboles doivent être séparés par des espaces des deux côtés.

``` cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** Lors de l'utilisation de types de modèles, les alias avec le `using` mot-clé (sauf dans les cas les plus simples).

En d'autres termes, les paramètres du modèle sont indiquées que dans `using` et ne sont pas répétés dans le code.

`using` peut être déclaré localement, comme dans une fonction.

``` cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** Ne déclarez pas plusieurs variables de types différents dans une instruction.

``` cpp
//incorrect
int x, *y;
```

**28.** N'utilisez pas de moulages de style C.

``` cpp
//incorrect
std::cerr << (int)c <<; std::endl;
//correct
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** Dans les classes et les structures, groupez les membres et les fonctions séparément dans chaque portée de visibilité.

**30.** Pour les petites classes et structures, il n'est pas nécessaire de séparer la déclaration de méthode de l'implémentation.

La même chose est vraie pour les petites méthodes dans toutes les classes ou structures.

Pour les classes et les structures modélisées, ne séparez pas les déclarations de méthode de l'implémentation (car sinon elles doivent être définies dans la même unité de traduction).

**31.** Vous pouvez envelopper des lignes à 140 caractères, au lieu de 80.

**32.** Utilisez toujours les opérateurs d'incrémentation/décrémentation de préfixe si postfix n'est pas requis.

``` cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

## Commentaire {#comments}

**1.** Assurez-vous d'ajouter des commentaires pour toutes les parties non triviales du code.

C'est très important. Écrit le commentaire peut vous aider à réaliser que le code n'est pas nécessaire, ou qu'il est mal conçu.

``` cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to position right after those 10 bytes available for read).
  */
```

**2.** Les commentaires peuvent être aussi détaillées que nécessaire.

**3.** Placez les commentaires avant le code qu'ils décrivent. Dans de rares cas, des commentaires peuvent venir après le code, sur la même ligne.

``` cpp
/** Parses and executes the query.
*/
void executeQuery(
    ReadBuffer & istr, /// Where to read the query from (and data for INSERT, if applicable)
    WriteBuffer & ostr, /// Where to write the result
    Context & context, /// DB, tables, data types, engines, functions, aggregate functions...
    BlockInputStreamPtr & query_plan, /// Here could be written the description on how query was executed
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete /// Up to which stage process the SELECT query
    )
```

**4.** Les commentaires doivent être rédigés en anglais seulement.

**5.** Si vous écrivez une bibliothèque, incluez des commentaires détaillés l'expliquant dans le fichier d'en-tête principal.

**6.** N'ajoutez pas de commentaires qui ne fournissent pas d'informations supplémentaires. En particulier, ne laissez pas de commentaires vides comme celui-ci:

``` cpp
/*
* Procedure Name:
* Original procedure name:
* Author:
* Date of creation:
* Dates of modification:
* Modification authors:
* Original file name:
* Purpose:
* Intent:
* Designation:
* Classes used:
* Constants:
* Local variables:
* Parameters:
* Date of creation:
* Purpose:
*/
```

L'exemple est emprunté à partir de la ressource http://home.tamk.fi/~jaalto/cours/coding-style/doc/désuète-code/.

**7.** Ne pas écrire des commentaires de déchets (auteur, date de création .. au début de chaque fichier.

**8.** Les commentaires sur une seule ligne commencent par trois barres obliques: `///` et les commentaires multi-lignes commencer avec `/**`. Ces commentaires sont pris en considération “documentation”.

REMARQUE: Vous pouvez utiliser Doxygen pour générer de la documentation à partir de ces commentaires. Mais Doxygen n'est généralement pas utilisé car il est plus pratique de naviguer dans le code dans L'IDE.

**9.** Les commentaires multilignes ne doivent pas avoir de lignes vides au début et à la fin (sauf la ligne qui ferme un commentaire multilignes).

**10.** Pour commenter le code, utilisez des commentaires de base, pas “documenting” commentaire.

**11.** Supprimez les parties commentées du code avant de valider.

**12.** N'utilisez pas de blasphème dans les commentaires ou le code.

**13.** N'utilisez pas de majuscules. N'utilisez pas de ponctuation excessive.

``` cpp
/// WHAT THE FAIL???
```

**14.** N'utilisez pas de commentaires pour créer des délimiteurs.

``` cpp
///******************************************************
```

**15.** Ne commencez pas les discussions dans les commentaires.

``` cpp
/// Why did you do this stuff?
```

**16.** Il n'est pas nécessaire d'écrire un commentaire à la fin d'un bloc décrivant de quoi il s'agissait.

``` cpp
/// for
```

## Nom {#names}

**1.** Utilisez des lettres minuscules avec des traits de soulignement dans les noms des variables et des membres de la classe.

``` cpp
size_t max_block_size;
```

**2.** Pour les noms de fonctions (méthodes), utilisez camelCase commençant par une lettre minuscule.

``` cpp
std::string getName() const override { return "Memory"; }
```

**3.** Pour les noms de classes (structures), utilisez CamelCase commençant par une lettre majuscule. Les préfixes autres que I ne sont pas utilisés pour les interfaces.

``` cpp
class StorageMemory : public IStorage
```

**4.** `using` sont nommées de la même manière que les classes, ou avec `_t` sur la fin.

**5.** Noms des arguments de type de modèle: dans les cas simples, utilisez `T`; `T`, `U`; `T1`, `T2`.

Pour les cas plus complexes, suivez les règles pour les noms de classe ou ajoutez le préfixe `T`.

``` cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** Noms des arguments constants du modèle: suivez les règles pour les noms de variables ou utilisez `N` dans les cas simples.

``` cpp
template <bool without_www>
struct ExtractDomain
```

**7.** Pour les classes abstraites (interfaces), vous pouvez ajouter `I` préfixe.

``` cpp
class IBlockInputStream
```

**8.** Si vous utilisez une variable localement, vous pouvez utiliser le nom court.

Dans tous les autres cas, utilisez un nom qui décrit la signification.

``` cpp
bool info_successfully_loaded = false;
```

**9.** Les noms de `define`les constantes s et globales utilisent ALL\_CAPS avec des traits de soulignement.

``` cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** Les noms de fichiers doivent utiliser le même style que leur contenu.

Si un fichier contient une seule classe, nommez-le de la même manière que la classe (CamelCase).

Si le fichier contient une seule fonction, nommez le fichier de la même manière que la fonction (camelCase).

**11.** Si le nom contient une abréviation, puis:

-   Pour les noms de variables, l'abréviation doit utiliser des lettres minuscules `mysql_connection` (pas `mySQL_connection`).
-   Pour les noms de classes et de fonctions, conservez les majuscules dans l'abréviation`MySQLConnection` (pas `MySqlConnection`).

**12.** Les arguments du constructeur utilisés uniquement pour initialiser les membres de la classe doivent être nommés de la même manière que les membres de la classe, mais avec un trait de soulignement à la fin.

``` cpp
FileQueueProcessor(
    const std::string & path_,
    const std::string & prefix_,
    std::shared_ptr<FileHandler> handler_)
    : path(path_),
    prefix(prefix_),
    handler(handler_),
    log(&Logger::get("FileQueueProcessor"))
{
}
```

Le suffixe de soulignement peut être omis si l'argument n'est pas utilisé dans le corps du constructeur.

**13.** Il n'y a pas de différence dans les noms des variables locales et des membres de classe (aucun préfixe requis).

``` cpp
timer (not m_timer)
```

**14.** Pour les constantes dans un `enum`, utilisez CamelCase avec une lettre majuscule. ALL\_CAPS est également acceptable. Si l' `enum` est non local, utilisez un `enum class`.

``` cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** Tous les noms doivent être en anglais. La translittération des mots russes n'est pas autorisé.

    not Stroka

**16.** Les abréviations sont acceptables si elles sont bien connues (quand vous pouvez facilement trouver la signification de l'abréviation dans Wikipédia ou dans un moteur de recherche).

    `AST`, `SQL`.

    Not `NVDH` (some random letters)

Les mots incomplets sont acceptables si la version abrégée est d'usage courant.

Vous pouvez également utiliser une abréviation si le nom complet est ensuite incluse dans les commentaires.

**17.** Les noms de fichiers avec le code source C++ doivent avoir `.cpp` extension. Fichiers d'en-tête doit avoir la `.h` extension.

## Comment écrire du Code {#how-to-write-code}

**1.** Gestion de la mémoire.

Désallocation manuelle de la mémoire (`delete`) ne peut être utilisé que dans le code de la bibliothèque.

Dans le code de la bibliothèque, de la `delete` l'opérateur ne peut être utilisé dans des destructeurs.

Dans le code de l'application, la mémoire doit être libérée par l'objet qui la possède.

Exemple:

-   Le plus simple est de placer un objet sur la pile, ou d'en faire un membre d'une autre classe.
-   Pour un grand nombre de petits objets, utiliser des récipients.
-   Pour la désallocation automatique d'un petit nombre d'objets qui résident dans le tas, utilisez `shared_ptr/unique_ptr`.

**2.** La gestion des ressources.

Utiliser `RAII` et voir ci-dessus.

**3.** La gestion des erreurs.

Utilisez des exceptions. Dans la plupart des cas, vous avez seulement besoin de lancer une exception, et n'avez pas besoin de l'attraper (à cause de `RAII`).

Dans les applications de traitement de données hors ligne, il est souvent acceptable de ne pas attraper d'exceptions.

Dans les serveurs qui gèrent les demandes des utilisateurs, il suffit généralement d'attraper des exceptions au niveau supérieur du gestionnaire de connexion.

Dans les fonctions de thread, vous devez attraper et conserver toutes les exceptions pour les repasser dans le thread principal après `join`.

``` cpp
/// If there weren't any calculations yet, calculate the first block synchronously
if (!started)
{
    calculate();
    started = true;
}
else /// If calculations are already in progress, wait for the result
    pool.wait();

if (exception)
    exception->rethrow();
```

Ne cachez jamais les exceptions sans les manipuler. Ne mettez jamais aveuglément toutes les exceptions au journal.

``` cpp
//Not correct
catch (...) {}
```

Si vous devez ignorer certaines exceptions, ne le faites que pour des exceptions spécifiques et repensez le reste.

``` cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

Lorsque vous utilisez des fonctions avec des codes de réponse ou `errno` toujours vérifier le résultat et de lever une exception en cas d'erreur.

``` cpp
if (0 != close(fd))
    throwFromErrno("Cannot close file " + file_name, ErrorCodes::CANNOT_CLOSE_FILE);
```

`Do not use assert`.

**4.** Les types d'Exception.

Il n'est pas nécessaire d'utiliser une hiérarchie d'exceptions complexe dans le code de l'application. Le texte d'exception doit être compréhensible pour un administrateur système.

**5.** Lancer des exceptions de destructeurs.

Ce n'est pas recommandé, mais il est permis.

Utilisez les options suivantes:

-   Créer une fonction (`done()` ou `finalize()`) qui vont faire tout le travail en amont qui pourrait conduire à une exception. Si cette fonction a été appelée, il ne devrait y avoir aucune exception dans le destructeur plus tard.
-   Les tâches trop complexes (comme l'envoi de messages sur le réseau) peuvent être placées dans une méthode distincte que l'utilisateur de la classe devra appeler avant la destruction.
-   Si il y a une exception dans le destructeur, il est préférable de l'enregistrer que de le cacher (si l'enregistreur est disponible).
-   Dans les applications simples, il est acceptable de compter sur `std::terminate` (pour les cas de `noexcept` par défaut en C++11) pour gérer les exceptions.

**6.** Blocs de code anonymes.

Vous pouvez créer un bloc de code séparé à l'intérieur d'une seule fonction afin de rendre certaines variables locales, de sorte que les destructeurs sont appelés à la sortie du bloc.

``` cpp
Block block = data.in->read();

{
    std::lock_guard<std::mutex> lock(mutex);
    data.ready = true;
    data.block = block;
}

ready_any.set();
```

**7.** Multithreading.

Dans les programmes de traitement de données hors ligne:

-   Essayez d'obtenir les meilleures performances possibles sur un seul noyau CPU. Vous pouvez ensuite paralléliser votre code si nécessaire.

Dans les applications serveur:

-   Utiliser le pool de threads pour traiter les demandes. À ce stade, nous n'avons pas eu de tâches nécessitant un changement de contexte dans l'espace utilisateur.

La fourche n'est pas utilisé pour la parallélisation.

**8.** Synchronisation des threads.

Souvent, il est possible de faire en sorte que différents threads utilisent différentes cellules de mémoire (encore mieux: différentes lignes de cache,) et de ne pas utiliser de synchronisation de thread (sauf `joinAll`).

Si la synchronisation est nécessaire, dans la plupart des cas, il suffit d'utiliser mutex sous `lock_guard`.

Dans d'autres cas, utilisez des primitives de synchronisation système. Ne pas utiliser occupé attendre.

Les opérations atomiques ne doivent être utilisées que dans les cas les plus simples.

N'essayez pas d'implémenter des structures de données sans verrou à moins qu'il ne s'agisse de votre principal domaine d'expertise.

**9.** Pointeurs vs références.

Dans la plupart des cas, préférez les références.

**10.** const.

Utiliser des références constantes, des pointeurs vers des constantes, `const_iterator` et const méthodes.

Considérer `const` pour être par défaut et utiliser non-`const` seulement quand c'est nécessaire.

Lors du passage de variables par valeur, en utilisant `const` habituellement ne fait pas de sens.

**11.** non signé.

Utiliser `unsigned` si nécessaire.

**12.** Les types numériques.

Utiliser les types `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, et `Int64` ainsi que `size_t`, `ssize_t`, et `ptrdiff_t`.

N'utilisez pas ces types pour les nombres: `signed/unsigned long`, `long long`, `short`, `signed/unsigned char`, `char`.

**13.** Passer des arguments.

Passer des valeurs complexes par référence (y compris `std::string`).

Si une fonction capture la propriété d'un objet créé dans le tas, définissez le type d'argument `shared_ptr` ou `unique_ptr`.

**14.** Les valeurs de retour.

Dans la plupart des cas, il suffit d'utiliser `return`. Ne pas écrire `[return std::move(res)]{.strike}`.

Si la fonction alloue un objet sur le tas et le renvoie, utilisez `shared_ptr` ou `unique_ptr`.

Dans de rares cas, vous devrez peut-être renvoyer la valeur via un argument. Dans ce cas, l'argument doit être une référence.

``` cpp
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

/** Allows creating an aggregate function by its name.
  */
class AggregateFunctionFactory
{
public:
    AggregateFunctionFactory();
    AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;
```

**15.** espace de noms.

Il n'est pas nécessaire d'utiliser une `namespace` pour le code de l'application.

Les petites bibliothèques n'ont pas besoin de cela non plus.

Pour les bibliothèques moyennes et grandes, mettez tout dans un `namespace`.

Dans la bibliothèque `.h` fichier, vous pouvez utiliser `namespace detail` pour masquer les détails d'implémentation non nécessaires pour le code de l'application.

Dans un `.cpp` fichier, vous pouvez utiliser un `static` ou un espace de noms anonyme pour masquer les symboles.

Aussi, un `namespace` peut être utilisé pour un `enum` pour éviter que les noms correspondants ne tombent dans un `namespace` (mais il est préférable d'utiliser un `enum class`).

**16.** Initialisation différée.

Si des arguments sont requis pour l'initialisation, vous ne devriez normalement pas écrire de constructeur par défaut.

Si plus tard, vous devez retarder l'initialisation, vous pouvez ajouter un constructeur par défaut qui créera un objet invalide. Ou, pour un petit nombre d'objets, vous pouvez utiliser `shared_ptr/unique_ptr`.

``` cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For deferred initialization
Loader() {}
```

**17.** Des fonctions virtuelles.

Si la classe n'est pas destinée à une utilisation polymorphe, vous n'avez pas besoin de rendre les fonctions virtuelles. Ceci s'applique également pour le destructeur.

**18.** Encodage.

Utilisez UTF-8 partout. Utiliser `std::string`et`char *`. Ne pas utiliser de `std::wstring`et`wchar_t`.

**19.** Journalisation.

Voir les exemples partout dans le code.

Avant de valider, supprimez toute journalisation sans signification et de débogage, ainsi que tout autre type de sortie de débogage.

L'enregistrement des cycles doit être évité, même au niveau de la Trace.

Les journaux doivent être lisibles à tout niveau d'enregistrement.

La journalisation ne doit être utilisée que dans le code de l'application, pour la plupart.

Les messages du journal doivent être écrits en anglais.

Le journal devrait de préférence être compréhensible pour l'administrateur système.

N'utilisez pas de blasphème dans le journal.

Utilisez L'encodage UTF-8 dans le journal. Dans de rares cas, vous pouvez utiliser des caractères non-ASCII dans le journal.

**20.** D'entrée-sortie.

Ne pas utiliser de `iostreams` dans les cycles internes qui sont critiques pour les performances de l'application (et ne jamais utiliser `stringstream`).

L'utilisation de la `DB/IO` la bibliothèque la place.

**21.** La Date et l'heure.

Voir la `DateLUT` bibliothèque.

**22.** comprendre.

Toujours utiliser `#pragma once` au lieu d'inclure des gardes.

**23.** utiliser.

`using namespace` n'est pas utilisé. Vous pouvez utiliser `using` avec quelque chose de spécifique. Mais faire local à l'intérieur d'une classe ou d'une fonction.

**24.** Ne pas utiliser de `trailing return type` pour les fonctions, sauf si nécessaire.

``` cpp
[auto f() -&gt; void;]{.strike}
```

**25.** Déclaration et initialisation des variables.

``` cpp
//right way
std::string s = "Hello";
std::string s{"Hello"};

//wrong way
auto s = std::string{"Hello"};
```

**26.** Pour les fonctions virtuelles, écrire `virtual` dans la classe de base, mais d'écrire `override` plutôt `virtual` dans les classes descendantes.

## Fonctionnalités inutilisées de C++ {#unused-features-of-c}

**1.** L'héritage virtuel n'est pas utilisé.

**2.** Les spécificateurs d'Exception de C++03 ne sont pas utilisés.

## Plate {#platform}

**1.** Nous écrivons du code pour une plate-forme spécifique.

Mais toutes choses étant égales par ailleurs, le code multi-plateforme ou portable est préféré.

**2.** Langue: C++17.

**3.** Compilateur: `gcc`. En ce moment (décembre 2017), le code est compilé en utilisant la version 7.2. (Il peut également être compilé en utilisant `clang 4`.)

La bibliothèque standard est utilisée (`libstdc++` ou `libc++`).

**4.**OS: Linux Ubuntu, pas plus vieux que précis.

**5.**Le Code est écrit pour l'architecture CPU x86\_64.

Le jeu D'instructions CPU est l'ensemble minimum pris en charge parmi nos serveurs. Actuellement, il s'agit de SSE 4.2.

**6.** Utiliser `-Wall -Wextra -Werror` drapeaux de compilation.

**7.** Utilisez la liaison statique avec toutes les bibliothèques sauf celles qui sont difficiles à connecter statiquement (voir la sortie de la `ldd` commande).

**8.** Le Code est développé et débogué avec les paramètres de version.

## Outils {#tools}

**1.** KDevelop est un bon IDE.

**2.** Pour le débogage, utilisez `gdb`, `valgrind` (`memcheck`), `strace`, `-fsanitize=...`, ou `tcmalloc_minimal_debug`.

**3.** Pour le profilage, utilisez `Linux Perf`, `valgrind` (`callgrind`), ou `strace -cf`.

**4.** Les Sources sont dans Git.

**5.** Assemblée utilise `CMake`.

**6.** Les programmes sont libérés en utilisant `deb` paquet.

**7.** Les Commits à master ne doivent pas casser la construction.

Bien que seules les révisions sélectionnées soient considérées comme réalisables.

**8.** Faire s'engage aussi souvent que possible, même si le code n'est que partiellement prêt.

Utilisez des branches à cet effet.

Si votre code dans le `master` la branche n'est pas constructible pourtant, l'exclure de la construction avant que le `push`. Vous devrez le terminer ou l'enlever dans quelques jours.

**9.** Pour les modifications non triviales, utilisez les branches et publiez-les sur le serveur.

**10.** Le code inutilisé est supprimé du référentiel.

## Bibliothèque {#libraries}

**1.** La bibliothèque standard C++14 est utilisée (les extensions expérimentales sont autorisées), ainsi que `boost` et `Poco` Framework.

**2.** Si nécessaire, vous pouvez utiliser toutes les bibliothèques bien connues disponibles dans le package OS.

S'il existe déjà une bonne solution, utilisez-la, même si cela signifie que vous devez installer une autre bibliothèque.

(Mais soyez prêt à supprimer les mauvaises bibliothèques du code.)

**3.** Vous pouvez installer une bibliothèque qui n'est pas dans les paquets, les paquets n'ont pas ce que vous souhaitez ou avez une version périmée ou le mauvais type de compilation.

**4.** Si la Bibliothèque est petite et n'a pas son propre système de construction complexe, placez les fichiers source dans le `contrib` dossier.

**5.** La préférence est toujours donnée aux bibliothèques déjà utilisées.

## Recommandations Générales {#general-recommendations-1}

**1.** Écrivez aussi peu de code que possible.

**2.** Essayez la solution la plus simple.

**3.** N'écrivez pas de code tant que vous ne savez pas comment cela va fonctionner et comment la boucle interne fonctionnera.

**4.** Dans les cas les plus simples, utilisez `using` au lieu de classes ou des structures.

**5.** Si possible, n'écrivez pas de constructeurs de copie, d'opérateurs d'affectation, de destructeurs (autres que Virtuels, si la classe contient au moins une fonction virtuelle), de constructeurs de déplacement ou d'opérateurs d'affectation de déplacement. En d'autres termes, les fonctions générées par le compilateur doivent fonctionner correctement. Vous pouvez utiliser `default`.

**6.** La simplification du Code est encouragée. Réduire la taille de votre code si possible.

## Recommandations Supplémentaires {#additional-recommendations}

**1.** Spécifier explicitement `std::` pour les types de `stddef.h`

n'est pas recommandé. En d'autres termes, nous vous recommandons d'écriture `size_t` plutôt `std::size_t` parce que c'est plus court.

Il est acceptable d'ajouter `std::`.

**2.** Spécifier explicitement `std::` pour les fonctions de la bibliothèque C standard

n'est pas recommandé. En d'autres termes, écrire `memcpy` plutôt `std::memcpy`.

La raison en est qu'il existe des fonctions non standard similaires, telles que `memmem`. Nous utilisons ces fonctions à l'occasion. Ces fonctions n'existent pas dans `namespace std`.

Si vous écrivez `std::memcpy` plutôt `memcpy` partout, puis `memmem` sans `std::` va sembler étrange.

Néanmoins, vous pouvez toujours utiliser `std::` si vous le souhaitez.

**3.** Utilisation des fonctions de C lorsque les mêmes sont disponibles dans la bibliothèque C++ standard.

Ceci est acceptable s'il est plus efficace.

Par exemple, l'utilisation `memcpy` plutôt `std::copy` pour copier de gros morceaux de mémoire.

**4.** Arguments de fonction multiligne.

L'un des styles d'emballage suivants est autorisé:

``` cpp
function(
  T1 x1,
  T2 x2)
```

``` cpp
function(
  size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

``` cpp
function(size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

``` cpp
function(size_t left, size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```

``` cpp
function(
      size_t left,
      size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```

[Article Original](https://clickhouse.tech/docs/en/development/style/) <!--hide-->
