#include <cassert>

#include <Parsers/obfuscateQueries.h>
#include <Parsers/Lexer.h>
#include <Poco/String.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/BitHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromMemory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_TEMPORARY_COLUMNS;
}


namespace
{

const std::unordered_set<std::string_view> keywords
{
    "CREATE",       "DATABASE",   "IF",       "NOT",      "EXISTS",    "TEMPORARY", "TABLE",       "ON",         "CLUSTER",     "DEFAULT",
    "MATERIALIZED", "EPHEMERAL",  "ALIAS",    "ENGINE",   "AS",        "VIEW",      "POPULATE",    "SETTINGS",   "ATTACH",      "DETACH",
    "DROP",         "RENAME",     "TO",       "ALTER",    "ADD",       "MODIFY",    "CLEAR",       "COLUMN",     "AFTER",       "COPY",
    "PROJECT",      "PRIMARY",    "KEY",      "CHECK",    "PARTITION", "PART",      "FREEZE",      "FETCH",      "FROM",        "SHOW",
    "INTO",         "OUTFILE",    "FORMAT",   "TABLES",   "DATABASES", "LIKE",      "PROCESSLIST", "CASE",       "WHEN",        "THEN",
    "ELSE",         "END",        "DESCRIBE", "DESC",     "USE",       "SET",       "OPTIMIZE",    "FINAL",      "DEDUPLICATE", "INSERT",
    "VALUES",       "SELECT",     "DISTINCT", "SAMPLE",   "ARRAY",     "JOIN",      "GLOBAL",      "LOCAL",      "ANY",         "ALL",
    "INNER",        "LEFT",       "RIGHT",    "FULL",     "OUTER",     "CROSS",     "USING",       "PREWHERE",   "WHERE",       "GROUP",
    "BY",           "WITH",       "TOTALS",   "HAVING",   "ORDER",     "COLLATE",   "LIMIT",       "UNION",      "AND",         "OR",
    "ASC",          "IN",         "KILL",     "QUERY",    "SYNC",      "ASYNC",     "TEST",        "BETWEEN",    "TRUNCATE",    "USER",
    "ROLE",         "PROFILE",    "QUOTA",    "POLICY",   "ROW",       "GRANT",     "REVOKE",      "OPTION",     "ADMIN",       "EXCEPT",
    "REPLACE",      "IDENTIFIED", "HOST",     "NAME",     "READONLY",  "WRITABLE",  "PERMISSIVE",  "FOR",        "RESTRICTIVE", "RANDOMIZED",
    "INTERVAL",     "LIMITS",     "ONLY",     "TRACKING", "IP",        "REGEXP",    "ILIKE",       "DICTIONARY", "OFFSET",      "TRIM",
    "LTRIM",        "RTRIM",      "BOTH",     "LEADING",  "TRAILING"
};

const std::unordered_set<std::string_view> keep_words
{
    "id", "name", "value", "num",
    "Id", "Name", "Value", "Num",
    "ID", "NAME", "VALUE", "NUM",
};

/// The list of nouns collected from here: http://www.desiquintans.com/nounlist, Public domain.
std::initializer_list<std::string_view> nouns
{
"aardvark", "abacus", "abbey", "abbreviation", "abdomen", "ability", "abnormality", "abolishment", "abortion",
"abrogation", "absence", "abundance", "abuse", "academics", "academy", "accelerant", "accelerator", "accent", "acceptance", "access",
"accessory", "accident", "accommodation", "accompanist", "accomplishment", "accord", "accordance", "accordion", "account", "accountability",
"accountant", "accounting", "accuracy", "accusation", "acetate", "achievement", "achiever", "acid", "acknowledgment", "acorn", "acoustics",
"acquaintance", "acquisition", "acre", "acrylic", "act", "action", "activation", "activist", "activity", "actor", "actress", "acupuncture",
"ad", "adaptation", "adapter", "addiction", "addition", "address", "adjective", "adjustment", "admin", "administration", "administrator",
"admire", "admission", "adobe", "adoption", "adrenalin", "adrenaline", "adult", "adulthood", "advance", "advancement", "advantage", "advent",
"adverb", "advertisement", "advertising", "advice", "adviser", "advocacy", "advocate", "affair", "affect", "affidavit", "affiliate",
"affinity", "afoul", "afterlife", "aftermath", "afternoon", "aftershave", "aftershock", "afterthought", "age", "agency", "agenda", "agent",
"aggradation", "aggression", "aglet", "agony", "agreement", "agriculture", "aid", "aide", "aim", "air", "airbag", "airbus", "aircraft",
"airfare", "airfield", "airforce", "airline", "airmail", "airman", "airplane", "airport", "airship", "airspace", "alarm", "alb", "albatross",
"album", "alcohol", "alcove", "alder", "ale", "alert", "alfalfa", "algebra", "algorithm", "alibi", "alien", "allegation", "allergist",
"alley", "alliance", "alligator", "allocation", "allowance", "alloy", "alluvium", "almanac", "almighty", "almond", "alpaca", "alpenglow",
"alpenhorn", "alpha", "alphabet", "altar", "alteration", "alternative", "altitude", "alto", "aluminium", "aluminum", "amazement", "amazon",
"ambassador", "amber", "ambience", "ambiguity", "ambition", "ambulance", "amendment", "amenity", "ammunition", "amnesty", "amount", "amusement",
"anagram", "analgesia", "analog", "analogue", "analogy", "analysis", "analyst", "analytics", "anarchist", "anarchy", "anatomy", "ancestor",
"anchovy", "android", "anesthesiologist", "anesthesiology", "angel", "anger", "angina", "angiosperm", "angle", "angora", "angstrom",
"anguish", "animal", "anime", "anise", "ankle", "anklet", "anniversary", "announcement", "annual", "anorak", "answer", "ant", "anteater",
"antecedent", "antechamber", "antelope", "antennae", "anterior", "anthropology", "antibody", "anticipation", "anticodon", "antigen",
"antique", "antiquity", "antler", "antling", "anxiety", "anybody", "anyone", "anything", "anywhere", "apartment", "ape", "aperitif",
"apology", "app", "apparatus", "apparel", "appeal", "appearance", "appellation", "appendix", "appetiser", "appetite", "appetizer", "applause",
"apple", "applewood", "appliance", "application", "appointment", "appreciation", "apprehension", "approach", "appropriation", "approval",
"apricot", "apron", "apse", "aquarium", "aquifer", "arcade", "arch", "archaeologist", "archaeology", "archeology", "archer",
"architect", "architecture", "archives", "area", "arena", "argument", "arithmetic", "ark", "arm", "armadillo", "armament",
"armchair", "armoire", "armor", "armour", "armpit", "armrest", "army", "arrangement", "array", "arrest", "arrival", "arrogance", "arrow",
"art", "artery", "arthur", "artichoke", "article", "artifact", "artificer", "artist", "ascend", "ascent", "ascot", "ash", "ashram", "ashtray",
"aside", "asparagus", "aspect", "asphalt", "aspic", "assassination", "assault", "assembly", "assertion", "assessment", "asset",
"assignment", "assist", "assistance", "assistant", "associate", "association", "assumption", "assurance", "asterisk", "astrakhan", "astrolabe",
"astrologer", "astrology", "astronomy", "asymmetry", "atelier", "atheist", "athlete", "athletics", "atmosphere", "atom", "atrium", "attachment",
"attack", "attacker", "attainment", "attempt", "attendance", "attendant", "attention", "attenuation", "attic", "attitude", "attorney",
"attraction", "attribute", "auction", "audience", "audit", "auditorium", "aunt", "authentication", "authenticity", "author", "authorisation",
"authority", "authorization", "auto", "autoimmunity", "automation", "automaton", "autumn", "availability", "avalanche", "avenue", "average",
"avocado", "award", "awareness", "awe", "axis", "azimuth", "babe", "baboon", "babushka", "baby", "bachelor", "back", "backbone",
"backburn", "backdrop", "background", "backpack", "backup", "backyard", "bacon", "bacterium", "badge", "badger", "bafflement", "bag",
"bagel", "baggage", "baggie", "baggy", "bagpipe", "bail", "bait", "bake", "baker", "bakery", "bakeware", "balaclava", "balalaika", "balance",
"balcony", "ball", "ballet", "balloon", "balloonist", "ballot", "ballpark", "bamboo", "ban", "banana", "band", "bandana", "bandanna",
"bandolier", "bandwidth", "bangle", "banjo", "bank", "bankbook", "banker", "banking", "bankruptcy", "banner", "banquette", "banyan",
"baobab", "bar", "barbecue", "barbeque", "barber", "barbiturate", "bargain", "barge", "baritone", "barium", "bark", "barley", "barn",
"barometer", "barracks", "barrage", "barrel", "barrier", "barstool", "bartender", "base", "baseball", "baseboard", "baseline", "basement",
"basics", "basil", "basin", "basis", "basket", "basketball", "bass", "bassinet", "bassoon", "bat", "bath", "bather", "bathhouse", "bathrobe",
"bathroom", "bathtub", "battalion", "batter", "battery", "batting", "battle", "battleship", "bay", "bayou", "beach", "bead", "beak",
"beam", "bean", "beancurd", "beanie", "beanstalk", "bear", "beard", "beast", "beastie", "beat", "beating", "beauty", "beaver", "beck",
"bed", "bedrock", "bedroom", "bee", "beech", "beef", "beer", "beet", "beetle", "beggar", "beginner", "beginning", "begonia", "behalf",
"behavior", "behaviour", "beheading", "behest", "behold", "being", "belfry", "belief", "believer", "bell", "belligerency", "bellows",
"belly", "belt", "bench", "bend", "beneficiary", "benefit", "beret", "berry", "bestseller", "bet", "beverage", "beyond",
"bias", "bibliography", "bicycle", "bid", "bidder", "bidding", "bidet", "bifocals", "bijou", "bike", "bikini", "bill", "billboard", "billing",
"billion", "bin", "binoculars", "biology", "biopsy", "biosphere", "biplane", "birch", "bird", "birdbath", "birdcage",
"birdhouse", "birth", "birthday", "biscuit", "bit", "bite", "bitten", "bitter", "black", "blackberry", "blackbird", "blackboard", "blackfish",
"blackness", "bladder", "blade", "blame", "blank", "blanket", "blast", "blazer", "blend", "blessing", "blight", "blind", "blinker", "blister",
"blizzard", "block", "blocker", "blog", "blogger", "blood", "bloodflow", "bloom", "bloomer", "blossom", "blouse", "blow", "blowgun",
"blowhole", "blue", "blueberry", "blush", "boar", "board", "boat", "boatload", "boatyard", "bob", "bobcat", "body", "bog", "bolero",
"bolt", "bomb", "bomber", "bombing", "bond", "bonding", "bondsman", "bone", "bonfire", "bongo", "bonnet", "bonsai", "bonus", "boogeyman",
"book", "bookcase", "bookend", "booking", "booklet", "bookmark", "boolean", "boom", "boon", "boost", "booster", "boot", "bootee", "bootie",
"booty", "border", "bore", "borrower", "borrowing", "bosom", "boss", "botany", "bother", "bottle", "bottling", "bottom",
"boudoir", "bough", "boulder", "boulevard", "boundary", "bouquet", "bourgeoisie", "bout", "boutique", "bow", "bower", "bowl", "bowler",
"bowling", "bowtie", "box", "boxer", "boxspring", "boy", "boycott", "boyfriend", "boyhood", "boysenberry", "bra", "brace", "bracelet",
"bracket", "brain", "brake", "bran", "branch", "brand", "brandy", "brass", "brassiere", "bratwurst", "bread", "breadcrumb", "breadfruit",
"break", "breakdown", "breakfast", "breakpoint", "breakthrough", "breast", "breastplate", "breath", "breeze", "brewer", "bribery", "brick",
"bricklaying", "bride", "bridge", "brief", "briefing", "briefly", "briefs", "brilliant", "brink", "brisket", "broad", "broadcast", "broccoli",
"brochure", "brocolli", "broiler", "broker", "bronchitis", "bronco", "bronze", "brooch", "brood", "brook", "broom", "brother",
"brow", "brown", "brownie", "browser", "browsing", "brunch", "brush", "brushfire", "brushing", "bubble", "buck", "bucket", "buckle",
"buckwheat", "bud", "buddy", "budget", "buffalo", "buffer", "buffet", "bug", "buggy", "bugle", "builder", "building", "bulb", "bulk",
"bull", "bulldozer", "bullet", "bump", "bumper", "bun", "bunch", "bungalow", "bunghole", "bunkhouse", "burden", "bureau",
"burglar", "burial", "burlesque", "burn", "burning", "burrito", "burro", "burrow", "burst", "bus", "bush", "business", "businessman",
"bust", "bustle", "butane", "butcher", "butler", "butter", "butterfly", "button", "buy", "buyer", "buying", "buzz", "buzzard",
"cabana", "cabbage", "cabin", "cabinet", "cable", "caboose", "cacao", "cactus", "caddy", "cadet", "cafe", "caffeine", "caftan", "cage",
"cake", "calcification", "calculation", "calculator", "calculus", "calendar", "calf", "caliber", "calibre", "calico", "call", "calm",
"calorie", "camel", "cameo", "camera", "camp", "campaign", "campaigning", "campanile", "camper", "campus", "can", "canal", "cancer",
"candelabra", "candidacy", "candidate", "candle", "candy", "cane", "cannibal", "cannon", "canoe", "canon", "canopy", "cantaloupe", "canteen",
"canvas", "cap", "capability", "capacity", "cape", "caper", "capital", "capitalism", "capitulation", "capon", "cappelletti", "cappuccino",
"captain", "caption", "captor", "car", "carabao", "caramel", "caravan", "carbohydrate", "carbon", "carboxyl", "card", "cardboard", "cardigan",
"care", "career", "cargo", "caribou", "carload", "carnation", "carnival", "carol", "carotene", "carp", "carpenter", "carpet", "carpeting",
"carport", "carriage", "carrier", "carrot", "carry", "cart", "cartel", "carter", "cartilage", "cartload", "cartoon", "cartridge", "carving",
"cascade", "casement", "cash", "cashew", "cashier", "casino", "casket", "cassava", "casserole", "cassock", "cast", "castanet",
"castle", "casualty", "cat", "catacomb", "catalogue", "catalysis", "catalyst", "catamaran", "catastrophe", "catch", "catcher", "category",
"caterpillar", "cathedral", "cation", "catsup", "cattle", "cauliflower", "causal", "cause", "causeway", "caution", "cave", "caviar",
"cayenne", "ceiling", "celebration", "celebrity", "celeriac", "celery", "cell", "cellar", "cello", "celsius", "cement", "cemetery", "cenotaph",
"census", "cent", "center", "centimeter", "centre", "centurion", "century", "cephalopod", "ceramic", "ceramics", "cereal", "ceremony",
"certainty", "certificate", "certification", "cesspool", "chafe", "chain", "chainstay", "chair", "chairlift", "chairman", "chairperson",
"chaise", "chalet", "chalice", "chalk", "challenge", "chamber", "champagne", "champion", "championship", "chance", "chandelier", "change",
"channel", "chaos", "chap", "chapel", "chaplain", "chapter", "character", "characteristic", "characterization", "chard", "charge", "charger",
"charity", "charlatan", "charm", "charset", "chart", "charter", "chasm", "chassis", "chastity", "chasuble", "chateau", "chatter", "chauffeur",
"chauvinist", "check", "checkbook", "checking", "checkout", "checkroom", "cheddar", "cheek", "cheer", "cheese", "cheesecake", "cheetah",
"chef", "chem", "chemical", "chemistry", "chemotaxis", "cheque", "cherry", "chess", "chest", "chestnut", "chick", "chicken", "chicory",
"chief", "chiffonier", "child", "childbirth", "childhood", "chili", "chill", "chime", "chimpanzee", "chin", "chinchilla", "chino", "chip",
"chipmunk", "chivalry", "chive", "chives", "chocolate", "choice", "choir", "choker", "cholesterol", "choosing", "chop",
"chops", "chopstick", "chopsticks", "chord", "chorus", "chow", "chowder", "chrome", "chromolithograph", "chronicle", "chronograph", "chronometer",
"chrysalis", "chub", "chuck", "chug", "church", "churn", "chutney", "cicada", "cigarette", "cilantro", "cinder", "cinema", "cinnamon",
"circadian", "circle", "circuit", "circulation", "circumference", "circumstance", "cirrhosis", "cirrus", "citizen", "citizenship", "citron",
"citrus", "city", "civilian", "civilisation", "civilization", "claim", "clam", "clamp", "clan", "clank", "clapboard", "clarification",
"clarinet", "clarity", "clasp", "class", "classic", "classification", "classmate", "classroom", "clause", "clave", "clavicle", "clavier",
"claw", "clay", "cleaner", "clearance", "clearing", "cleat", "cleavage", "clef", "cleft", "clergyman", "cleric", "clerk", "click", "client",
"cliff", "climate", "climb", "clinic", "clip", "clipboard", "clipper", "cloak", "cloakroom", "clock", "clockwork", "clogs", "cloister",
"clone", "close", "closet", "closing", "closure", "cloth", "clothes", "clothing", "cloud", "cloudburst", "clove", "clover", "cloves",
"club", "clue", "cluster", "clutch", "coach", "coal", "coalition", "coast", "coaster", "coat", "cob", "cobbler", "cobweb",
"cock", "cockpit", "cockroach", "cocktail", "cocoa", "coconut", "cod", "code", "codepage", "codling", "codon", "codpiece", "coevolution",
"cofactor", "coffee", "coffin", "cohesion", "cohort", "coil", "coin", "coincidence", "coinsurance", "coke", "cold", "coleslaw", "coliseum",
"collaboration", "collagen", "collapse", "collar", "collard", "collateral", "colleague", "collection", "collectivisation", "collectivization",
"collector", "college", "collision", "colloquy", "colon", "colonial", "colonialism", "colonisation", "colonization", "colony", "color",
"colorlessness", "colt", "column", "columnist", "comb", "combat", "combination", "combine", "comeback", "comedy", "comestible", "comfort",
"comfortable", "comic", "comics", "comma", "command", "commander", "commandment", "comment", "commerce", "commercial", "commission",
"commitment", "committee", "commodity", "common", "commonsense", "commotion", "communicant", "communication", "communion", "communist",
"community", "commuter", "company", "comparison", "compass", "compassion", "compassionate", "compensation", "competence", "competition",
"competitor", "complaint", "complement", "completion", "complex", "complexity", "compliance", "complication", "complicity", "compliment",
"component", "comportment", "composer", "composite", "composition", "compost", "comprehension", "compress", "compromise", "comptroller",
"compulsion", "computer", "comradeship", "con", "concentrate", "concentration", "concept", "conception", "concern", "concert", "conclusion",
"concrete", "condition", "conditioner", "condominium", "condor", "conduct", "conductor", "cone", "confectionery", "conference", "confidence",
"confidentiality", "configuration", "confirmation", "conflict", "conformation", "confusion", "conga", "congo", "congregation", "congress",
"congressman", "congressperson", "conifer", "connection", "connotation", "conscience", "consciousness", "consensus", "consent", "consequence",
"conservation", "conservative", "consideration", "consignment", "consist", "consistency", "console", "consonant", "conspiracy", "conspirator",
"constant", "constellation", "constitution", "constraint", "construction", "consul", "consulate", "consulting", "consumer", "consumption",
"contact", "contact lens", "contagion", "container", "content", "contention", "contest", "context", "continent", "contingency", "continuity",
"contour", "contract", "contractor", "contrail", "contrary", "contrast", "contribution", "contributor", "control", "controller", "controversy",
"convection", "convenience", "convention", "conversation", "conversion", "convert", "convertible", "conviction", "cook", "cookbook",
"cookie", "cooking", "coonskin", "cooperation", "coordination", "coordinator", "cop", "cope", "copper", "copy", "copying",
"copyright", "copywriter", "coral", "cord", "corduroy", "core", "cork", "cormorant", "corn", "corner", "cornerstone", "cornet", "cornflakes",
"cornmeal", "corporal", "corporation", "corporatism", "corps", "corral", "correspondence", "correspondent", "corridor", "corruption",
"corsage", "cosset", "cost", "costume", "cot", "cottage", "cotton", "couch", "cougar", "cough", "council", "councilman", "councilor",
"councilperson", "counsel", "counseling", "counselling", "counsellor", "counselor", "count", "counter", "counterpart",
"counterterrorism", "countess", "country", "countryside", "county", "couple", "coupon", "courage", "course", "court", "courthouse", "courtroom",
"cousin", "covariate", "cover", "coverage", "coverall", "cow", "cowbell", "cowboy", "coyote", "crab", "crack", "cracker", "crackers",
"cradle", "craft", "craftsman", "cranberry", "crane", "cranky", "crash", "crate", "cravat", "craw", "crawdad", "crayfish", "crayon",
"crazy", "cream", "creation", "creationism", "creationist", "creative", "creativity", "creator", "creature", "creche", "credential",
"credenza", "credibility", "credit", "creditor", "creek", "creme brulee", "crepe", "crest", "crew", "crewman", "crewmate", "crewmember",
"crewmen", "cria", "crib", "cribbage", "cricket", "cricketer", "crime", "criminal", "crinoline", "crisis", "crisp", "criteria", "criterion",
"critic", "criticism", "crocodile", "crocus", "croissant", "crook", "crop", "cross", "crotch",
"croup", "crow", "crowd", "crown", "crucifixion", "crude", "cruelty", "cruise", "crumb", "crunch", "crusader", "crush", "crust", "cry",
"crystal", "crystallography", "cub", "cube", "cuckoo", "cucumber", "cue", "cuisine", "cultivar", "cultivator", "culture",
"culvert", "cummerbund", "cup", "cupboard", "cupcake", "cupola", "curd", "cure", "curio", "curiosity", "curl", "curler", "currant", "currency",
"current", "curriculum", "curry", "curse", "cursor", "curtailment", "curtain", "curve", "cushion", "custard", "custody", "custom", "customer",
"cut", "cuticle", "cutlet", "cutover", "cutting", "cyclamen", "cycle", "cyclone", "cyclooxygenase", "cygnet", "cylinder", "cymbal", "cynic",
"cyst", "cytokine", "cytoplasm", "dad", "daddy", "daffodil", "dagger", "dahlia", "daikon", "daily", "dairy", "daisy", "dam", "damage",
"dame", "dance", "dancer", "dancing", "dandelion", "danger", "dare", "dark", "darkness", "darn", "dart", "dash", "dashboard",
"data", "date", "daughter", "dawn", "day", "daybed", "daylight", "dead", "deadline", "deal", "dealer", "dealing", "dearest",
"death", "deathwatch", "debate", "debris", "debt", "debtor", "decade", "decadence", "decency", "decimal", "decision",
"deck", "declaration", "declination", "decline", "decoder", "decongestant", "decoration", "decrease", "decryption", "dedication", "deduce",
"deduction", "deed", "deep", "deer", "defeat", "defendant", "defender", "defense", "deficit", "definition", "deformation",
"degradation", "degree", "delay", "deliberation", "delight", "delivery", "demand", "democracy", "democrat", "demon", "demur", "den",
"denim", "denominator", "density", "dentist", "deodorant", "department", "departure", "dependency", "dependent", "deployment", "deposit",
"deposition", "depot", "depression", "depressive", "depth", "deputy", "derby", "derivation", "derivative", "derrick", "descendant", "descent",
"description", "desert", "design", "designation", "designer", "desire", "desk", "desktop", "dessert", "destination", "destiny", "destroyer",
"destruction", "detail", "detainee", "detainment", "detection", "detective", "detector", "detention", "determination", "detour", "devastation",
"developer", "developing", "development", "developmental", "deviance", "deviation", "device", "devil", "dew", "dhow", "diabetes", "diadem",
"diagnosis", "diagram", "dial", "dialect", "dialogue", "diam", "diamond", "diaper", "diaphragm", "diarist", "diary", "dibble", "dickey", "dictaphone", "dictator", "diction", "dictionary", "die", "diesel", "diet", "difference", "differential", "difficulty", "diffuse",
"dig", "digestion", "digestive", "digger", "digging", "digit", "dignity", "dilapidation", "dill", "dilution", "dime", "dimension", "dimple",
"diner", "dinghy", "dining", "dinner", "dinosaur", "dioxide", "dip", "diploma", "diplomacy", "dipstick", "direction", "directive", "director",
"directory", "dirndl", "dirt", "disability", "disadvantage", "disagreement", "disappointment", "disarmament", "disaster", "discharge",
"discipline", "disclaimer", "disclosure", "disco", "disconnection", "discount", "discourse", "discovery", "discrepancy", "discretion",
"discrimination", "discussion", "disdain", "disease", "disembodiment", "disengagement", "disguise", "disgust", "dish", "dishwasher",
"disk", "disparity", "dispatch", "displacement", "display", "disposal", "disposer", "disposition", "dispute", "disregard", "disruption",
"dissemination", "dissonance", "distance", "distinction", "distortion", "distribution", "distributor", "district", "divalent", "divan",
"diver", "diversity", "divide", "dividend", "divider", "divine", "diving", "division", "divorce", "doc", "dock", "doctor", "doctorate",
"doctrine", "document", "documentary", "documentation", "doe", "dog", "doggie", "dogsled", "dogwood", "doing", "doll", "dollar", "dollop",
"dolman", "dolor", "dolphin", "domain", "dome", "domination", "donation", "donkey", "donor", "donut", "door", "doorbell", "doorknob",
"doorpost", "doorway", "dory", "dose", "dot", "double", "doubling", "doubt", "doubter", "dough", "doughnut", "down", "downfall", "downforce",
"downgrade", "download", "downstairs", "downtown", "downturn", "dozen", "draft", "drag", "dragon", "dragonfly", "dragonfruit", "dragster",
"drain", "drainage", "drake", "drama", "dramaturge", "drapes", "draw", "drawbridge", "drawer", "drawing", "dream", "dreamer", "dredger",
"dress", "dresser", "dressing", "drill", "drink", "drinking", "drive", "driver", "driveway", "driving", "drizzle", "dromedary", "drop",
"drudgery", "drug", "drum", "drummer", "drunk", "dryer", "duck", "duckling", "dud", "dude", "due", "duel", "dueling", "duffel", "dugout",
"dulcimer", "dumbwaiter", "dump", "dump truck", "dune", "dune buggy", "dungarees", "dungeon", "duplexer", "duration", "durian", "dusk",
"dust", "dust storm", "duster", "duty", "dwarf", "dwell", "dwelling", "dynamics", "dynamite", "dynamo", "dynasty", "dysfunction",
"eagle", "eaglet", "ear", "eardrum", "earmuffs", "earnings", "earplug", "earring", "earrings", "earth", "earthquake",
"earthworm", "ease", "easel", "east", "eating", "eaves", "eavesdropper", "ecclesia", "echidna", "eclipse", "ecliptic", "ecology", "economics",
"economy", "ecosystem", "ectoderm", "ectodermal", "ecumenist", "eddy", "edge", "edger", "edible", "editing", "edition", "editor", "editorial",
"education", "eel", "effacement", "effect", "effective", "effectiveness", "effector", "efficacy", "efficiency", "effort", "egg", "egghead",
"eggnog", "eggplant", "ego", "eicosanoid", "ejector", "elbow", "elderberry", "election", "electricity", "electrocardiogram", "electronics",
"element", "elephant", "elevation", "elevator", "eleventh", "elf", "elicit", "eligibility", "elimination", "elite", "elixir", "elk",
"ellipse", "elm", "elongation", "elver", "email", "emanate", "embarrassment", "embassy", "embellishment", "embossing", "embryo", "emerald",
"emergence", "emergency", "emergent", "emery", "emission", "emitter", "emotion", "emphasis", "empire", "employ", "employee", "employer",
"employment", "empowerment", "emu", "enactment", "encirclement", "enclave", "enclosure", "encounter", "encouragement", "encyclopedia",
"end", "endive", "endoderm", "endorsement", "endothelium", "endpoint", "enemy", "energy", "enforcement", "engagement", "engine", "engineer",
"engineering", "enigma", "enjoyment", "enquiry", "enrollment", "enterprise", "entertainment", "enthusiasm", "entirety", "entity", "entrance",
"entree", "entrepreneur", "entry", "envelope", "environment", "envy", "enzyme", "epauliere", "epee", "ephemera", "ephemeris", "ephyra",
"epic", "episode", "epithelium", "epoch", "eponym", "epoxy", "equal", "equality", "equation", "equinox", "equipment", "equity", "equivalent",
"era", "eraser", "erection", "erosion", "error", "escalator", "escape", "escort", "espadrille", "espalier", "essay", "essence", "essential",
"establishment", "estate", "estimate", "estrogen", "estuary", "eternity", "ethernet", "ethics", "ethnicity", "ethyl", "euphonium", "eurocentrism",
"evaluation", "evaluator", "evaporation", "eve", "evening", "event", "everybody", "everyone", "everything", "eviction",
"evidence", "evil", "evocation", "evolution", "exaggeration", "exam", "examination", "examiner", "example",
"exasperation", "excellence", "exception", "excerpt", "excess", "exchange", "excitement", "exclamation", "excursion", "excuse", "execution",
"executive", "executor", "exercise", "exhaust", "exhaustion", "exhibit", "exhibition", "exile", "existence", "exit", "exocrine", "expansion",
"expansionism", "expectancy", "expectation", "expedition", "expense", "experience", "experiment", "experimentation", "expert", "expertise",
"explanation", "exploration", "explorer", "explosion", "export", "expose", "exposition", "exposure", "expression", "extension", "extent",
"exterior", "external", "extinction", "extreme", "extremist", "eye", "eyeball", "eyebrow", "eyebrows", "eyeglasses", "eyelash", "eyelashes",
"eyelid", "eyelids", "eyeliner", "eyestrain", "eyrie", "fabric", "face", "facelift", "facet", "facility", "facsimile", "fact", "factor",
"factory", "faculty", "fahrenheit", "fail", "failure", "fairness", "fairy", "faith", "faithful", "fall", "fallacy", "fame",
"familiar", "familiarity", "family", "fan", "fang", "fanlight", "fanny", "fantasy", "farm", "farmer", "farming", "farmland",
"farrow", "fascia", "fashion", "fat", "fate", "father", "fatigue", "fatigues", "faucet", "fault", "fav", "fava", "favor",
"favorite", "fawn", "fax", "fear", "feast", "feather", "feature", "fedelini", "federation", "fedora", "fee", "feed", "feedback", "feeding",
"feel", "feeling", "fellow", "felony", "female", "fen", "fence", "fencing", "fender", "feng", "fennel", "ferret", "ferry", "ferryboat",
"fertilizer", "festival", "fetus", "few", "fiber", "fiberglass", "fibre", "fibroblast", "fibrosis", "ficlet", "fiction", "fiddle", "field",
"fiery", "fiesta", "fifth", "fig", "fight", "fighter", "figure", "figurine", "file", "filing", "fill", "fillet", "filly", "film", "filter",
"filth", "final", "finance", "financing", "finding", "fine", "finer", "finger", "fingerling", "fingernail", "finish", "finisher", "fir",
"fire", "fireman", "fireplace", "firewall", "firm", "first", "fish", "fishbone", "fisherman", "fishery", "fishing", "fishmonger", "fishnet",
"fisting", "fit", "fitness", "fix", "fixture", "flag", "flair", "flame", "flan", "flanker", "flare", "flash", "flat", "flatboat", "flavor",
"flax", "fleck", "fledgling", "fleece", "flesh", "flexibility", "flick", "flicker", "flight", "flint", "flintlock", "flock",
"flood", "floodplain", "floor", "floozie", "flour", "flow", "flower", "flu", "flugelhorn", "fluke", "flume", "flung", "flute", "fly",
"flytrap", "foal", "foam", "fob", "focus", "fog", "fold", "folder", "folk", "folklore", "follower", "following", "fondue", "font", "food",
"foodstuffs", "fool", "foot", "footage", "football", "footnote", "footprint", "footrest", "footstep", "footstool", "footwear", "forage",
"forager", "foray", "force", "ford", "forearm", "forebear", "forecast", "forehead", "foreigner", "forelimb", "forest", "forestry", "forever",
"forgery", "fork", "form", "formal", "formamide", "formation", "former", "formicarium", "formula", "fort", "forte", "fortnight",
"fortress", "fortune", "forum", "foundation", "founder", "founding", "fountain", "fourths", "fowl", "fox", "foxglove", "fraction", "fragrance",
"frame", "framework", "fratricide", "fraud", "fraudster", "freak", "freckle", "freedom", "freelance", "freezer", "freezing", "freight",
"freighter", "frenzy", "freon", "frequency", "fresco", "friction", "fridge", "friend", "friendship", "fries", "frigate", "fright", "fringe",
"fritter", "frock", "frog", "front", "frontier", "frost", "frosting", "frown", "fruit", "frustration", "fry", "fuel", "fugato",
"fulfillment", "full", "fun", "function", "functionality", "fund", "funding", "fundraising", "funeral", "fur", "furnace", "furniture",
"furry", "fusarium", "futon", "future", "gadget", "gaffe", "gaffer", "gain", "gaiters", "gale", "gallery", "galley",
"gallon", "galoshes", "gambling", "game", "gamebird", "gaming", "gander", "gang", "gap", "garage", "garb", "garbage", "garden",
"garlic", "garment", "garter", "gas", "gasket", "gasoline", "gasp", "gastronomy", "gastropod", "gate", "gateway", "gather", "gathering",
"gator", "gauge", "gauntlet", "gavel", "gazebo", "gazelle", "gear", "gearshift", "geek", "gel", "gelatin", "gelding", "gem", "gemsbok",
"gender", "gene", "general", "generation", "generator", "generosity", "genetics", "genie", "genius", "genocide", "genre", "gentleman",
"geography", "geology", "geometry", "geranium", "gerbil", "gesture", "geyser", "gherkin", "ghost", "giant", "gift", "gig", "gigantism",
"giggle", "ginger", "gingerbread", "ginseng", "giraffe", "girdle", "girl", "girlfriend", "git", "glacier", "gladiolus", "glance", "gland",
"glass", "glasses", "glee", "glen", "glider", "gliding", "glimpse", "globe", "glockenspiel", "gloom", "glory", "glove", "glow", "glucose",
"glue", "glut", "glutamate", "gnat", "gnu", "goal", "goat", "gobbler", "god", "goddess", "godfather", "godmother", "godparent",
"goggles", "going", "gold", "goldfish", "golf", "gondola", "gong", "good", "goodbye", "goodie", "goodness", "goodnight",
"goodwill", "goose", "gopher", "gorilla", "gosling", "gossip", "governance", "government", "governor", "gown", "grace", "grade",
"gradient", "graduate", "graduation", "graffiti", "graft", "grain", "gram", "grammar", "gran", "grand", "grandchild", "granddaughter",
"grandfather", "grandma", "grandmom", "grandmother", "grandpa", "grandparent", "grandson", "granny", "granola", "grant", "grape", "grapefruit",
"graph", "graphic", "grasp", "grass", "grasshopper", "grassland", "gratitude", "gravel", "gravitas", "gravity", "gravy", "gray", "grease",
"greatness", "greed", "green", "greenhouse", "greens", "grenade", "grey", "grid", "grief",
"grill", "grin", "grip", "gripper", "grit", "grocery", "ground", "grouper", "grouse", "grove", "growth", "grub", "guacamole",
"guarantee", "guard", "guava", "guerrilla", "guess", "guest", "guestbook", "guidance", "guide", "guideline", "guilder", "guilt", "guilty",
"guinea", "guitar", "guitarist", "gum", "gumshoe", "gun", "gunpowder", "gutter", "guy", "gym", "gymnast", "gymnastics", "gynaecology",
"gyro", "habit", "habitat", "hacienda", "hacksaw", "hackwork", "hail", "hair", "haircut", "hake", "half",
"halibut", "hall", "halloween", "hallway", "halt", "ham", "hamburger", "hammer", "hammock", "hamster", "hand", "handball",
"handful", "handgun", "handicap", "handle", "handlebar", "handmaiden", "handover", "handrail", "handsaw", "hanger", "happening", "happiness",
"harald", "harbor", "harbour", "hardboard", "hardcover", "hardening", "hardhat", "hardship", "hardware", "hare", "harm",
"harmonica", "harmonise", "harmonize", "harmony", "harp", "harpooner", "harpsichord", "harvest", "harvester", "hash", "hashtag", "hassock",
"haste", "hat", "hatbox", "hatchet", "hatchling", "hate", "hatred", "haunt", "haven", "haversack", "havoc", "hawk", "hay", "haze", "hazel",
"hazelnut", "head", "headache", "headlight", "headline", "headphones", "headquarters", "headrest", "health", "hearing",
"hearsay", "heart", "heartache", "heartbeat", "hearth", "hearthside", "heartwood", "heat", "heater", "heating", "heaven",
"heavy", "hectare", "hedge", "hedgehog", "heel", "heifer", "height", "heir", "heirloom", "helicopter", "helium", "hell", "hellcat", "hello",
"helmet", "helo", "help", "hemisphere", "hemp", "hen", "hepatitis", "herb", "herbs", "heritage", "hermit", "hero", "heroine", "heron",
"herring", "hesitation", "hexagon", "heyday", "hiccups", "hide", "hierarchy", "high", "highland", "highlight",
"highway", "hike", "hiking", "hill", "hint", "hip", "hippodrome", "hippopotamus", "hire", "hiring", "historian", "history", "hit", "hive",
"hobbit", "hobby", "hockey", "hoe", "hog", "hold", "holder", "hole", "holiday", "home", "homeland", "homeownership", "hometown", "homework",
"homicide", "homogenate", "homonym", "honesty", "honey", "honeybee", "honeydew", "honor", "honoree", "hood",
"hoof", "hook", "hop", "hope", "hops", "horde", "horizon", "hormone", "horn", "hornet", "horror", "horse", "horseradish", "horst", "hose",
"hosiery", "hospice", "hospital", "hospitalisation", "hospitality", "hospitalization", "host", "hostel", "hostess", "hotdog", "hotel",
"hound", "hour", "hourglass", "house", "houseboat", "household", "housewife", "housework", "housing", "hovel", "hovercraft", "howard",
"howitzer", "hub", "hubcap", "hubris", "hug", "hugger", "hull", "human", "humanity", "humidity", "hummus", "humor", "humour", "hunchback",
"hundred", "hunger", "hunt", "hunter", "hunting", "hurdle", "hurdler", "hurricane", "hurry", "hurt", "husband", "hut", "hutch", "hyacinth",
"hybridisation", "hybridization", "hydrant", "hydraulics", "hydrocarb", "hydrocarbon", "hydrofoil", "hydrogen", "hydrolyse", "hydrolysis",
"hydrolyze", "hydroxyl", "hyena", "hygienic", "hype", "hyphenation", "hypochondria", "hypothermia", "hypothesis", "ice",
"iceberg", "icebreaker", "icecream", "icicle", "icing", "icon", "icy", "id", "idea", "ideal", "identification", "identity", "ideology",
"idiom", "idiot", "igloo", "ignorance", "ignorant", "ikebana", "illegal", "illiteracy", "illness", "illusion", "illustration", "image",
"imagination", "imbalance", "imitation", "immigrant", "immigration", "immortal", "impact", "impairment", "impala", "impediment", "implement",
"implementation", "implication", "import", "importance", "impostor", "impress", "impression", "imprisonment", "impropriety", "improvement",
"impudence", "impulse", "inability", "inauguration", "inbox", "incandescence", "incarnation", "incense", "incentive",
"inch", "incidence", "incident", "incision", "inclusion", "income", "incompetence", "inconvenience", "increase", "incubation", "independence",
"independent", "index", "indication", "indicator", "indigence", "individual", "industrialisation", "industrialization", "industry", "inequality",
"inevitable", "infancy", "infant", "infarction", "infection", "infiltration", "infinite", "infix", "inflammation", "inflation", "influence",
"influx", "info", "information", "infrastructure", "infusion", "inglenook", "ingrate", "ingredient", "inhabitant", "inheritance", "inhibition",
"inhibitor", "initial", "initialise", "initialize", "initiative", "injunction", "injury", "injustice", "ink", "inlay", "inn", "innervation",
"innocence", "innocent", "innovation", "input", "inquiry", "inscription", "insect", "insectarium", "insert", "inside", "insight", "insolence",
"insomnia", "inspection", "inspector", "inspiration", "installation", "instance", "instant", "instinct", "institute", "institution",
"instruction", "instructor", "instrument", "instrumentalist", "instrumentation", "insulation", "insurance", "insurgence", "insurrection",
"integer", "integral", "integration", "integrity", "intellect", "intelligence", "intensity", "intent", "intention", "intentionality",
"interaction", "interchange", "interconnection", "intercourse", "interest", "interface", "interferometer", "interior", "interject", "interloper",
"internet", "interpretation", "interpreter", "interval", "intervenor", "intervention", "interview", "interviewer", "intestine", "introduction",
"intuition", "invader", "invasion", "invention", "inventor", "inventory", "inverse", "inversion", "investigation", "investigator", "investment",
"investor", "invitation", "invite", "invoice", "involvement", "iridescence", "iris", "iron", "ironclad", "irony", "irrigation", "ischemia",
"island", "isogloss", "isolation", "issue", "item", "itinerary", "ivory", "jack", "jackal", "jacket", "jackfruit", "jade", "jaguar",
"jail", "jailhouse", "jalapeño", "jam", "jar", "jasmine", "jaw", "jazz", "jealousy", "jeans", "jeep", "jelly", "jellybeans", "jellyfish",
"jerk", "jet", "jewel", "jeweller", "jewellery", "jewelry", "jicama", "jiffy", "job", "jockey", "jodhpurs", "joey", "jogging", "joint",
"joke", "jot", "journal", "journalism", "journalist", "journey", "joy", "judge", "judgment", "judo", "jug", "juggernaut", "juice", "julienne",
"jumbo", "jump", "jumper", "jumpsuit", "jungle", "junior", "junk", "junker", "junket", "jury", "justice", "justification", "jute", "kale",
"kamikaze", "kangaroo", "karate", "kayak", "kazoo", "kebab", "keep", "keeper", "kendo", "kennel", "ketch", "ketchup", "kettle", "kettledrum",
"key", "keyboard", "keyboarding", "keystone", "kick", "kid", "kidney", "kielbasa", "kill", "killer", "killing", "kilogram",
"kilometer", "kilt", "kimono", "kinase", "kind", "kindness", "king", "kingdom", "kingfish", "kiosk", "kiss", "kit", "kitchen", "kite",
"kitsch", "kitten", "kitty", "kiwi", "knee", "kneejerk", "knickers", "knife", "knight", "knitting", "knock", "knot",
"knowledge", "knuckle", "koala", "kohlrabi", "kumquat", "lab", "label", "labor", "laboratory", "laborer", "labour", "labourer", "lace",
"lack", "lacquerware", "lad", "ladder", "ladle", "lady", "ladybug", "lag", "lake", "lamb", "lambkin", "lament", "lamp", "lanai", "land",
"landform", "landing", "landmine", "landscape", "lane", "language", "lantern", "lap", "laparoscope", "lapdog", "laptop", "larch", "lard",
"larder", "lark", "larva", "laryngitis", "lasagna", "lashes", "last", "latency", "latex", "lathe", "latitude", "latte", "latter", "laugh",
"laughter", "laundry", "lava", "law", "lawmaker", "lawn", "lawsuit", "lawyer", "lay", "layer", "layout", "lead", "leader", "leadership",
"leading", "leaf", "league", "leaker", "leap", "learning", "leash", "leather", "leave", "leaver", "lecture", "leek", "leeway", "left",
"leg", "legacy", "legal", "legend", "legging", "legislation", "legislator", "legislature", "legitimacy", "legume", "leisure", "lemon",
"lemonade", "lemur", "lender", "lending", "length", "lens", "lentil", "leopard", "leprosy", "leptocephalus", "lesson", "letter",
"lettuce", "level", "lever", "leverage", "leveret", "liability", "liar", "liberty", "libido", "library", "licence", "license", "licensing",
"licorice", "lid", "lie", "lieu", "lieutenant", "life", "lifestyle", "lifetime", "lift", "ligand", "light", "lighting", "lightning",
"lightscreen", "ligula", "likelihood", "likeness", "lilac", "lily", "limb", "lime", "limestone", "limitation", "limo", "line",
"linen", "liner", "linguist", "linguistics", "lining", "link", "linkage", "linseed", "lion", "lip", "lipid", "lipoprotein", "lipstick",
"liquid", "liquidity", "liquor", "list", "listening", "listing", "literate", "literature", "litigation", "litmus", "litter", "littleneck",
"liver", "livestock", "living", "lizard", "llama", "load", "loading", "loaf", "loafer", "loan", "lobby", "lobotomy", "lobster", "local",
"locality", "location", "lock", "locker", "locket", "locomotive", "locust", "lode", "loft", "log", "loggia", "logic", "login", "logistics",
"logo", "loincloth", "lollipop", "loneliness", "longboat", "longitude", "look", "lookout", "loop", "loophole", "loquat", "lord", "loss",
"lot", "lotion", "lottery", "lounge", "louse", "lout", "love", "lover", "lox", "loyalty", "luck", "luggage", "lumber", "lumberman", "lunch",
"luncheonette", "lunchmeat", "lunchroom", "lung", "lunge", "lust", "lute", "luxury", "lychee", "lycra", "lye", "lymphocyte", "lynx",
"lyocell", "lyre", "lyrics", "lysine", "mRNA", "macadamia", "macaroni", "macaroon", "macaw", "machine", "machinery", "macrame", "macro",
"macrofauna", "madam", "maelstrom", "maestro", "magazine", "maggot", "magic", "magnet", "magnitude", "maid", "maiden", "mail", "mailbox",
"mailer", "mailing", "mailman", "main", "mainland", "mainstream", "maintainer", "maintenance", "maize", "major", "majority",
"makeover", "maker", "makeup", "making", "male", "malice", "mall", "mallard", "mallet", "malnutrition", "mama", "mambo", "mammoth", "man",
"manacle", "management", "manager", "manatee", "mandarin", "mandate", "mandolin", "mangle", "mango", "mangrove", "manhunt", "maniac",
"manicure", "manifestation", "manipulation", "mankind", "manner", "manor", "mansard", "manservant", "mansion", "mantel", "mantle", "mantua",
"manufacturer", "manufacturing", "many", "map", "maple", "mapping", "maracas", "marathon", "marble", "march", "mare", "margarine", "margin",
"mariachi", "marimba", "marines", "marionberry", "mark", "marker", "market", "marketer", "marketing", "marketplace", "marksman", "markup",
"marmalade", "marriage", "marsh", "marshland", "marshmallow", "marten", "marxism", "mascara", "mask", "masonry", "mass", "massage", "mast",
"master", "masterpiece", "mastication", "mastoid", "mat", "match", "matchmaker", "mate", "material", "maternity", "math", "mathematics",
"matrix", "matter", "mattock", "mattress", "max", "maximum", "maybe", "mayonnaise", "mayor", "meadow", "meal", "mean", "meander", "meaning",
"means", "meantime", "measles", "measure", "measurement", "meat", "meatball", "meatloaf", "mecca", "mechanic", "mechanism", "med", "medal",
"media", "median", "medication", "medicine", "medium", "meet", "meeting", "melatonin", "melody", "melon", "member", "membership", "membrane",
"meme", "memo", "memorial", "memory", "men", "menopause", "menorah", "mention", "mentor", "menu", "merchandise", "merchant", "mercury",
"meridian", "meringue", "merit", "mesenchyme", "mess", "message", "messenger", "messy", "metabolite", "metal", "metallurgist", "metaphor",
"meteor", "meteorology", "meter", "methane", "method", "methodology", "metric", "metro", "metronome", "mezzanine", "microlending", "micronutrient",
"microphone", "microwave", "midden", "middle", "middleman", "midline", "midnight", "midwife", "might", "migrant", "migration",
"mile", "mileage", "milepost", "milestone", "military", "milk", "milkshake", "mill", "millennium", "millet", "millimeter", "million",
"millisecond", "millstone", "mime", "mimosa", "min", "mincemeat", "mind", "mine", "mineral", "mineshaft", "mini", "minibus",
"minimalism", "minimum", "mining", "minion", "minister", "mink", "minnow", "minor", "minority", "mint", "minute", "miracle",
"mirror", "miscarriage", "miscommunication", "misfit", "misnomer", "misogyny", "misplacement", "misreading", "misrepresentation", "miss",
"missile", "mission", "missionary", "mist", "mistake", "mister", "misunderstand", "miter", "mitten", "mix", "mixer", "mixture", "moai",
"moat", "mob", "mobile", "mobility", "mobster", "moccasins", "mocha", "mochi", "mode", "model", "modeling", "modem", "modernist", "modernity",
"modification", "molar", "molasses", "molding", "mole", "molecule", "mom", "moment", "monastery", "monasticism", "money", "monger", "monitor",
"monitoring", "monk", "monkey", "monocle", "monopoly", "monotheism", "monsoon", "monster", "month", "monument", "mood", "moody", "moon",
"moonlight", "moonscape", "moonshine", "moose", "mop", "morale", "morbid", "morbidity", "morning", "moron", "morphology", "morsel", "mortal",
"mortality", "mortgage", "mortise", "mosque", "mosquito", "most", "motel", "moth", "mother", "motion", "motivation",
"motive", "motor", "motorboat", "motorcar", "motorcycle", "mound", "mountain", "mouse", "mouser", "mousse", "moustache", "mouth", "mouton",
"movement", "mover", "movie", "mower", "mozzarella", "mud", "muffin", "mug", "mukluk", "mule", "multimedia", "murder", "muscat", "muscatel",
"muscle", "musculature", "museum", "mushroom", "music", "musician", "muskrat", "mussel", "mustache", "mustard",
"mutation", "mutt", "mutton", "mycoplasma", "mystery", "myth", "mythology", "nail", "name", "naming", "nanoparticle", "napkin", "narrative",
"nasal", "nation", "nationality", "native", "naturalisation", "nature", "navigation", "necessity", "neck", "necklace", "necktie", "nectar",
"nectarine", "need", "needle", "neglect", "negligee", "negotiation", "neighbor", "neighborhood", "neighbour", "neighbourhood", "neologism",
"neon", "neonate", "nephew", "nerve", "nest", "nestling", "nestmate", "net", "netball", "netbook", "netsuke", "network", "networking",
"neurobiologist", "neuron", "neuropathologist", "neuropsychiatry", "news", "newsletter", "newspaper", "newsprint", "newsstand", "nexus",
"nibble", "nicety", "niche", "nick", "nickel", "nickname", "niece", "night", "nightclub", "nightgown", "nightingale", "nightlife", "nightlight",
"nightmare", "ninja", "nit", "nitrogen", "nobody", "nod", "node", "noir", "noise", "nonbeliever", "nonconformist", "nondisclosure", "nonsense",
"noodle", "noodles", "noon", "norm", "normal", "normalisation", "normalization", "north", "nose", "notation", "note", "notebook", "notepad",
"nothing", "notice", "notion", "notoriety", "nougat", "noun", "nourishment", "novel", "nucleotidase", "nucleotide", "nudge", "nuke",
"number", "numeracy", "numeric", "numismatist", "nun", "nurse", "nursery", "nursing", "nurture", "nut", "nutmeg", "nutrient", "nutrition",
"nylon", "nymph", "oak", "oar", "oasis", "oat", "oatmeal", "oats", "obedience", "obesity", "obi", "object", "objection", "objective",
"obligation", "oboe", "observation", "observatory", "obsession", "obsidian", "obstacle", "occasion", "occupation", "occurrence", "ocean",
"ocelot", "octagon", "octave", "octavo", "octet", "octopus", "odometer", "odyssey", "oeuvre", "offence", "offense", "offer",
"offering", "office", "officer", "official", "offset", "oil", "okra", "oldie", "oleo", "olive", "omega", "omelet", "omission", "omnivore",
"oncology", "onion", "online", "onset", "opening", "opera", "operating", "operation", "operator", "ophthalmologist", "opinion", "opium",
"opossum", "opponent", "opportunist", "opportunity", "opposite", "opposition", "optimal", "optimisation", "optimist", "optimization",
"option", "orange", "orangutan", "orator", "orchard", "orchestra", "orchid", "ordinary", "ordination", "ore", "oregano", "organ",
"organisation", "organising", "organization", "organizing", "orient", "orientation", "origin", "original", "originality", "ornament",
"osmosis", "osprey", "ostrich", "other", "otter", "ottoman", "ounce", "outback", "outcome", "outfielder", "outfit", "outhouse", "outlaw",
"outlay", "outlet", "outline", "outlook", "output", "outrage", "outrigger", "outrun", "outset", "outside", "oval", "ovary", "oven", "overcharge",
"overclocking", "overcoat", "overexertion", "overflight", "overhead", "overheard", "overload", "overnighter", "overshoot", "oversight",
"overview", "overweight", "owl", "owner", "ownership", "ox", "oxford", "oxygen", "oyster", "ozone", "pace", "pacemaker", "pack", "package",
"packaging", "packet", "pad", "paddle", "paddock", "pagan", "page", "pagoda", "pail", "pain", "paint", "painter", "painting", "paintwork",
"pair", "pajamas", "palace", "palate", "palm", "pamphlet", "pan", "pancake", "pancreas", "panda", "panel", "panic", "pannier", "panpipe",
"pansy", "panther", "panties", "pantologist", "pantology", "pantry", "pants", "pantsuit", "panty", "pantyhose", "papa", "papaya", "paper",
"paperback", "paperwork", "parable", "parachute", "parade", "paradise", "paragraph", "parallelogram", "paramecium", "paramedic", "parameter",
"paranoia", "parcel", "parchment", "pard", "pardon", "parent", "parenthesis", "parenting", "park", "parka", "parking", "parliament",
"parole", "parrot", "parser", "parsley", "parsnip", "part", "participant", "participation", "particle", "particular", "partner", "partnership",
"partridge", "party", "pass", "passage", "passbook", "passenger", "passing", "passion", "passive", "passport", "password", "past", "pasta",
"paste", "pastor", "pastoralist", "pastry", "pasture", "pat", "patch", "pate", "patent", "patentee", "path", "pathogenesis", "pathology",
"pathway", "patience", "patient", "patina", "patio", "patriarch", "patrimony", "patriot", "patrol", "patroller", "patrolling", "patron",
"pattern", "patty", "pattypan", "pause", "pavement", "pavilion", "paw", "pawnshop", "pay", "payee", "payment", "payoff", "pea", "peace",
"peach", "peacoat", "peacock", "peak", "peanut", "pear", "pearl", "peasant", "pecan", "pecker", "pedal", "peek", "peen", "peer",
"pegboard", "pelican", "pelt", "pen", "penalty", "pence", "pencil", "pendant", "pendulum", "penguin", "penicillin", "peninsula", "penis",
"pennant", "penny", "pension", "pentagon", "peony", "people", "pepper", "pepperoni", "percent", "percentage", "perception", "perch",
"perennial", "perfection", "performance", "perfume", "period", "periodical", "peripheral", "permafrost", "permission", "permit", "perp",
"perpendicular", "persimmon", "person", "personal", "personality", "personnel", "perspective", "pest", "pet", "petal", "petition", "petitioner",
"petticoat", "pew", "pharmacist", "pharmacopoeia", "phase", "pheasant", "phenomenon", "phenotype", "pheromone", "philanthropy", "philosopher",
"philosophy", "phone", "phosphate", "photo", "photodiode", "photograph", "photographer", "photography", "photoreceptor", "phrase", "phrasing",
"physical", "physics", "physiology", "pianist", "piano", "piccolo", "pick", "pickax", "pickaxe", "picket", "pickle", "pickup", "picnic",
"picture", "picturesque", "pie", "piece", "pier", "piety", "pig", "pigeon", "piglet", "pigpen", "pigsty", "pike", "pilaf", "pile", "pilgrim",
"pilgrimage", "pill", "pillar", "pillbox", "pillow", "pilot", "pimp", "pimple", "pin", "pinafore", "pine", "pineapple",
"pinecone", "ping", "pink", "pinkie", "pinot", "pinstripe", "pint", "pinto", "pinworm", "pioneer", "pipe", "pipeline", "piracy", "pirate",
"pistol", "pit", "pita", "pitch", "pitcher", "pitching", "pith", "pizza", "place", "placebo", "placement", "placode", "plagiarism",
"plain", "plaintiff", "plan", "plane", "planet", "planning", "plant", "plantation", "planter", "planula", "plaster", "plasterboard",
"plastic", "plate", "platelet", "platform", "platinum", "platter", "platypus", "play", "player", "playground", "playroom", "playwright",
"plea", "pleasure", "pleat", "pledge", "plenty", "plier", "pliers", "plight", "plot", "plough", "plover", "plow", "plowman", "plug",
"plugin", "plum", "plumber", "plume", "plunger", "plywood", "pneumonia", "pocket", "pocketbook", "pod", "podcast", "poem",
"poet", "poetry", "poignance", "point", "poison", "poisoning", "poker", "polarisation", "polarization", "pole", "polenta", "police",
"policeman", "policy", "polish", "politician", "politics", "poll", "polliwog", "pollutant", "pollution", "polo", "polyester", "polyp",
"pomegranate", "pomelo", "pompom", "poncho", "pond", "pony", "pool", "poor", "pop", "popcorn", "poppy", "popsicle", "popularity", "population",
"populist", "porcelain", "porch", "porcupine", "pork", "porpoise", "port", "porter", "portfolio", "porthole", "portion", "portrait",
"position", "possession", "possibility", "possible", "post", "postage", "postbox", "poster", "posterior", "postfix", "pot", "potato",
"potential", "pottery", "potty", "pouch", "poultry", "pound", "pounding", "poverty", "powder", "power", "practice", "practitioner", "prairie",
"praise", "pray", "prayer", "precedence", "precedent", "precipitation", "precision", "predecessor", "preface", "preference", "prefix",
"pregnancy", "prejudice", "prelude", "premeditation", "premier", "premise", "premium", "preoccupation", "preparation", "prescription",
"presence", "present", "presentation", "preservation", "preserves", "presidency", "president", "press", "pressroom", "pressure", "pressurisation",
"pressurization", "prestige", "presume", "pretzel", "prevalence", "prevention", "prey", "price", "pricing", "pride", "priest", "priesthood",
"primary", "primate", "prince", "princess", "principal", "principle", "print", "printer", "printing", "prior", "priority", "prison",
"prisoner", "privacy", "private", "privilege", "prize", "prizefight", "probability", "probation", "probe", "problem", "procedure", "proceedings",
"process", "processing", "processor", "proctor", "procurement", "produce", "producer", "product", "production", "productivity", "profession",
"professional", "professor", "profile", "profit", "progenitor", "program", "programme", "programming", "progress", "progression", "prohibition",
"project", "proliferation", "promenade", "promise", "promotion", "prompt", "pronoun", "pronunciation", "proof", "propaganda",
"propane", "property", "prophet", "proponent", "proportion", "proposal", "proposition", "proprietor", "prose", "prosecution", "prosecutor",
"prospect", "prosperity", "prostacyclin", "prostanoid", "prostrate", "protection", "protein", "protest", "protocol", "providence", "provider",
"province", "provision", "prow", "proximal", "proximity", "prune", "pruner", "pseudocode", "pseudoscience", "psychiatrist", "psychoanalyst",
"psychologist", "psychology", "ptarmigan", "pub", "public", "publication", "publicity", "publisher", "publishing", "pudding", "puddle",
"puffin", "pug", "puggle", "pulley", "pulse", "puma", "pump", "pumpernickel", "pumpkin", "pumpkinseed", "pun", "punch", "punctuation",
"punishment", "pup", "pupa", "pupil", "puppet", "puppy", "purchase", "puritan", "purity", "purple", "purpose", "purr", "purse", "pursuit",
"push", "pusher", "put", "puzzle", "pyramid", "pyridine", "quadrant", "quail", "qualification", "quality", "quantity", "quart", "quarter",
"quartet", "quartz", "queen", "query", "quest", "question", "questioner", "questionnaire", "quiche", "quicksand", "quiet", "quill", "quilt",
"quince", "quinoa", "quit", "quiver", "quota", "quotation", "quote", "rabbi", "rabbit", "raccoon", "race", "racer", "racing", "racism",
"racist", "rack", "radar", "radiator", "radio", "radiosonde", "radish", "raffle", "raft", "rag", "rage", "raid", "rail", "railing", "railroad",
"railway", "raiment", "rain", "rainbow", "raincoat", "rainmaker", "rainstorm", "rainy", "raise", "raisin", "rake", "rally", "ram", "rambler",
"ramen", "ramie", "ranch", "rancher", "randomisation", "randomization", "range", "ranger", "rank", "rap", "rape", "raspberry", "rat",
"rate", "ratepayer", "rating", "ratio", "rationale", "rations", "raven", "ravioli", "rawhide", "ray", "rayon", "razor", "reach", "reactant",
"reaction", "read", "reader", "readiness", "reading", "real", "reality", "realization", "realm", "reamer", "rear", "reason", "reasoning",
"rebel", "rebellion", "reboot", "recall", "recapitulation", "receipt", "receiver", "reception", "receptor", "recess", "recession", "recipe",
"recipient", "reciprocity", "reclamation", "recliner", "recognition", "recollection", "recommendation", "reconsideration", "record",
"recorder", "recording", "recovery", "recreation", "recruit", "rectangle", "red", "redesign", "redhead", "redirect", "rediscovery", "reduction",
"reef", "refectory", "reference", "referendum", "reflection", "reform", "refreshments", "refrigerator", "refuge", "refund", "refusal",
"refuse", "regard", "regime", "region", "regionalism", "register", "registration", "registry", "regret", "regulation", "regulator",
"rehospitalization", "reindeer", "reinscription", "reject", "relation", "relationship", "relative", "relaxation", "relay", "release",
"reliability", "relief", "religion", "relish", "reluctance", "remains", "remark", "reminder", "remnant", "remote", "removal", "renaissance",
"rent", "reorganisation", "reorganization", "repair", "reparation", "repayment", "repeat", "replacement", "replica", "replication", "reply",
"report", "reporter", "reporting", "repository", "representation", "representative", "reprocessing", "republic", "republican", "reputation",
"request", "requirement", "resale", "rescue", "research", "researcher", "resemblance", "reservation", "reserve", "reservoir", "reset",
"residence", "resident", "residue", "resist", "resistance", "resolution", "resolve", "resort", "resource", "respect", "respite", "response",
"responsibility", "rest", "restaurant", "restoration", "restriction", "restroom", "restructuring", "result", "resume", "retailer", "retention",
"rethinking", "retina", "retirement", "retouching", "retreat", "retrospect", "retrospective", "retrospectivity", "return", "reunion",
"revascularisation", "revascularization", "reveal", "revelation", "revenant", "revenge", "revenue", "reversal", "reverse", "review",
"revitalisation", "revitalization", "revival", "revolution", "revolver", "reward", "rhetoric", "rheumatism", "rhinoceros", "rhubarb",
"rhyme", "rhythm", "rib", "ribbon", "rice", "riddle", "ride", "rider", "ridge", "riding", "rifle", "right", "rim", "ring", "ringworm",
"riot", "rip", "ripple", "rise", "riser", "risk", "rite", "ritual", "river", "riverbed", "rivulet", "road", "roadway", "roar", "roast",
"robe", "robin", "robot", "robotics", "rock", "rocker", "rocket", "rod", "role", "roll", "roller", "romaine", "romance",
"roof", "room", "roommate", "rooster", "root", "rope", "rose", "rosemary", "roster", "rostrum", "rotation", "round", "roundabout", "route",
"router", "routine", "row", "rowboat", "rowing", "rubber", "rubric", "ruby", "ruckus", "rudiment", "ruffle", "rug", "rugby",
"ruin", "rule", "ruler", "ruling", "rum", "rumor", "run", "runaway", "runner", "running", "runway", "rush", "rust", "rutabaga", "rye",
"sabre", "sac", "sack", "saddle", "sadness", "safari", "safe", "safeguard", "safety", "saffron", "sage", "sail", "sailboat", "sailing",
"sailor", "saint", "sake", "salad", "salami", "salary", "sale", "salesman", "salmon", "salon", "saloon", "salsa", "salt", "salute", "samovar",
"sampan", "sample", "samurai", "sanction", "sanctity", "sanctuary", "sand", "sandal", "sandbar", "sandpaper", "sandwich", "sanity", "sardine",
"sari", "sarong", "sash", "satellite", "satin", "satire", "satisfaction", "sauce", "saucer", "sauerkraut", "sausage", "savage", "savannah",
"saving", "savings", "savior", "saviour", "savory", "saw", "saxophone", "scaffold", "scale", "scallion", "scallops", "scalp", "scam",
"scanner", "scarecrow", "scarf", "scarification", "scenario", "scene", "scenery", "scent", "schedule", "scheduling", "schema", "scheme",
"schizophrenic", "schnitzel", "scholar", "scholarship", "school", "schoolhouse", "schooner", "science", "scientist", "scimitar", "scissors",
"scooter", "scope", "score", "scorn", "scorpion", "scotch", "scout", "scow", "scrambled", "scrap", "scraper", "scratch", "screamer",
"screen", "screening", "screenwriting", "screw", "screwdriver", "scrim", "scrip", "script", "scripture", "scrutiny", "sculpting",
"sculptural", "sculpture", "sea", "seabass", "seafood", "seagull", "seal", "seaplane", "search", "seashore", "seaside", "season", "seat",
"seaweed", "second", "secrecy", "secret", "secretariat", "secretary", "secretion", "section", "sectional", "sector", "security", "sediment",
"seed", "seeder", "seeker", "seep", "segment", "seizure", "selection", "self", "seller",
"selling", "semantics", "semester", "semicircle", "semicolon", "semiconductor", "seminar", "senate", "senator", "sender", "senior", "sense",
"sensibility", "sensitive", "sensitivity", "sensor", "sentence", "sentencing", "sentiment", "sepal", "separation", "septicaemia", "sequel",
"sequence", "serial", "series", "sermon", "serum", "serval", "servant", "server", "service", "servitude", "sesame", "session", "set",
"setback", "setting", "settlement", "settler", "severity", "sewer", "sex", "sexuality", "shack", "shackle", "shade", "shadow", "shadowbox",
"shakedown", "shaker", "shallot", "shallows", "shame", "shampoo", "shanty", "shape", "share", "shareholder", "shark", "shaw", "shawl",
"shear", "shearling", "sheath", "shed", "sheep", "sheet", "shelf", "shell", "shelter", "sherbet", "sherry", "shield", "shift", "shin",
"shine", "shingle", "ship", "shipper", "shipping", "shipyard", "shirt", "shirtdress", "shoat", "shock", "shoe",
"shoehorn", "shoelace", "shoemaker", "shoes", "shoestring", "shofar", "shoot", "shootdown", "shop", "shopper", "shopping", "shore", "shoreline",
"short", "shortage", "shorts", "shortwave", "shot", "shoulder", "shout", "shovel", "show", "shower", "shred", "shrimp",
"shrine", "shutdown", "sibling", "sick", "sickness", "side", "sideboard", "sideburns", "sidecar", "sidestream", "sidewalk", "siding",
"siege", "sigh", "sight", "sightseeing", "sign", "signal", "signature", "signet", "significance", "signify", "signup", "silence", "silica",
"silicon", "silk", "silkworm", "sill", "silly", "silo", "silver", "similarity", "simple", "simplicity", "simplification", "simvastatin",
"sin", "singer", "singing", "singular", "sink", "sinuosity", "sip", "sir", "sister", "sitar", "site", "situation", "size",
"skate", "skating", "skean", "skeleton", "ski", "skiing", "skill", "skin", "skirt", "skull", "skullcap", "skullduggery", "skunk", "sky",
"skylight", "skyline", "skyscraper", "skywalk", "slang", "slapstick", "slash", "slate", "slavery", "slaw", "sled", "sledge",
"sleep", "sleepiness", "sleeping", "sleet", "sleuth", "slice", "slide", "slider", "slime", "slip", "slipper", "slippers", "slope", "slot",
"sloth", "slump", "smell", "smelting", "smile", "smith", "smock", "smog", "smoke", "smoking", "smolt", "smuggling", "snack", "snail",
"snake", "snakebite", "snap", "snarl", "sneaker", "sneakers", "sneeze", "sniffle", "snob", "snorer", "snow", "snowboarding", "snowflake",
"snowman", "snowmobiling", "snowplow", "snowstorm", "snowsuit", "snuck", "snug", "snuggle", "soap", "soccer", "socialism", "socialist",
"society", "sociology", "sock", "socks", "soda", "sofa", "softball", "softdrink", "softening", "software", "soil", "soldier", "sole",
"solicitation", "solicitor", "solidarity", "solidity", "soliloquy", "solitaire", "solution", "solvency", "sombrero", "somebody", "someone",
"someplace", "somersault", "something", "somewhere", "son", "sonar", "sonata", "song", "songbird", "sonnet", "soot", "sophomore", "soprano",
"sorbet", "sorghum", "sorrel", "sorrow", "sort", "soul", "soulmate", "sound", "soundness", "soup", "source", "sourwood", "sousaphone",
"south", "southeast", "souvenir", "sovereignty", "sow", "soy", "soybean", "space", "spacing", "spade", "spaghetti", "span", "spandex",
"spank", "sparerib", "spark", "sparrow", "spasm", "spat", "spatula", "spawn", "speaker", "speakerphone", "speaking", "spear", "spec",
"special", "specialist", "specialty", "species", "specification", "spectacle", "spectacles", "spectrograph", "spectrum", "speculation",
"speech", "speed", "speedboat", "spell", "spelling", "spelt", "spending", "sphere", "sphynx", "spice", "spider", "spiderling", "spike",
"spill", "spinach", "spine", "spiral", "spirit", "spiritual", "spirituality", "spit", "spite", "spleen", "splendor", "split", "spokesman",
"spokeswoman", "sponge", "sponsor", "sponsorship", "spool", "spoon", "spork", "sport", "sportsman", "spot", "spotlight", "spouse", "sprag",
"sprat", "spray", "spread", "spreadsheet", "spree", "spring", "sprinkles", "sprinter", "sprout", "spruce", "spud", "spume", "spur", "spy",
"spyglass", "square", "squash", "squatter", "squeegee", "squid", "squirrel", "stab", "stability", "stable", "stack", "stacking", "stadium",
"staff", "stag", "stage", "stain", "stair", "staircase", "stake", "stalk", "stall", "stallion", "stamen", "stamina", "stamp", "stance",
"stand", "standard", "standardisation", "standardization", "standing", "standoff", "standpoint", "star", "starboard", "start", "starter",
"state", "statement", "statin", "station", "statistic", "statistics", "statue", "status", "statute", "stay", "steak",
"stealth", "steam", "steamroller", "steel", "steeple", "stem", "stench", "stencil", "step",
"stepdaughter", "stepmother",
"stepson", "stereo", "stew", "steward", "stick", "sticker", "stiletto", "still", "stimulation", "stimulus", "sting",
"stinger", "stitch", "stitcher", "stock", "stockings", "stole", "stomach", "stone", "stonework", "stool",
"stop", "stopsign", "stopwatch", "storage", "store", "storey", "storm", "story", "storyboard", "stot", "stove", "strait",
"strand", "stranger", "strap", "strategy", "straw", "strawberry", "strawman", "stream", "street", "streetcar", "strength", "stress",
"stretch", "strife", "strike", "string", "strip", "stripe", "strobe", "stroke", "structure", "strudel", "struggle", "stucco", "stud",
"student", "studio", "study", "stuff", "stumbling", "stump", "stupidity", "sturgeon", "sty", "style", "styling", "stylus", "sub", "subcomponent",
"subconscious", "subcontractor", "subexpression", "subgroup", "subject", "submarine", "submitter", "subprime", "subroutine", "subscription",
"subsection", "subset", "subsidence", "subsidiary", "subsidy", "substance", "substitution", "subtitle", "suburb", "subway", "success",
"succotash", "suck", "sucker", "suede", "suet", "suffocation", "sugar", "suggestion", "suicide", "suit", "suitcase", "suite", "sulfur",
"sultan", "sum", "summary", "summer", "summit", "sun", "sunbeam", "sunbonnet", "sundae", "sunday", "sundial", "sunflower", "sunglasses",
"sunlamp", "sunlight", "sunrise", "sunroom", "sunset", "sunshine", "superiority", "supermarket", "supernatural", "supervision", "supervisor",
"supper", "supplement", "supplier", "supply", "support", "supporter", "suppression", "supreme", "surface", "surfboard", "surge", "surgeon",
"surgery", "surname", "surplus", "surprise", "surround", "surroundings", "surrounds", "survey", "survival", "survivor", "sushi", "suspect",
"suspenders", "suspension", "sustainment", "sustenance", "swallow", "swamp", "swan", "swanling", "swath", "sweat", "sweater", "sweatshirt",
"sweatshop", "sweatsuit", "sweets", "swell", "swim", "swimming", "swimsuit", "swine", "swing", "switch", "switchboard", "switching",
"swivel", "sword", "swordfight", "swordfish", "sycamore", "symbol", "symmetry", "sympathy", "symptom", "syndicate", "syndrome", "synergy",
"synod", "synonym", "synthesis", "syrup", "system", "tab", "tabby", "tabernacle", "tablecloth", "tablet", "tabletop",
"tachometer", "tackle", "taco", "tactics", "tactile", "tadpole", "tag", "tail", "tailbud", "tailor", "tailspin", "takeover",
"tale", "talent", "talk", "talking", "tamale", "tambour", "tambourine", "tan", "tandem", "tangerine", "tank",
"tanker", "tankful", "tap", "tape", "tapioca", "target", "taro", "tarragon", "tart", "task", "tassel", "taste", "tatami", "tattler",
"tattoo", "tavern", "tax", "taxi", "taxicab", "taxpayer", "tea", "teacher", "teaching", "team", "teammate", "teapot", "tear", "tech",
"technician", "technique", "technologist", "technology", "tectonics", "teen", "teenager", "teepee", "telephone", "telescreen", "teletype",
"television", "tell", "teller", "temp", "temper", "temperature", "temple", "tempo", "temporariness", "temporary", "temptation", "temptress",
"tenant", "tendency", "tender", "tenement", "tenet", "tennis", "tenor", "tension", "tensor", "tent", "tentacle", "tenth", "tepee", "teriyaki",
"term", "terminal", "termination", "terminology", "termite", "terrace", "terracotta", "terrapin", "terrarium", "territory", "terror",
"terrorism", "terrorist", "test", "testament", "testimonial", "testimony", "testing", "text", "textbook", "textual", "texture", "thanks",
"thaw", "theater", "theft", "theism", "theme", "theology", "theory", "therapist", "therapy", "thermals", "thermometer", "thermostat",
"thesis", "thickness", "thief", "thigh", "thing", "thinking", "thirst", "thistle", "thong", "thongs", "thorn", "thought", "thousand",
"thread", "threat", "threshold", "thrift", "thrill", "throat", "throne", "thrush", "thrust", "thug", "thumb", "thump", "thunder", "thunderbolt",
"thunderhead", "thunderstorm", "thyme", "tiara", "tic", "tick", "ticket", "tide", "tie", "tiger", "tights", "tile", "till", "tilt", "timbale",
"timber", "time", "timeline", "timeout", "timer", "timetable", "timing", "timpani", "tin", "tinderbox", "tinkle", "tintype", "tip", "tire",
"tissue", "titanium", "title", "toad", "toast", "toaster", "tobacco", "today", "toe", "toenail", "toffee", "tofu", "tog", "toga", "toilet",
"tolerance", "tolerant", "toll", "tomatillo", "tomato", "tomb", "tomography", "tomorrow", "ton", "tonality", "tone", "tongue",
"tonic", "tonight", "tool", "toot", "tooth", "toothbrush", "toothpaste", "toothpick", "top", "topic", "topsail", "toque",
"toreador", "tornado", "torso", "torte", "tortellini", "tortilla", "tortoise", "tosser", "total", "tote", "touch", "tour",
"tourism", "tourist", "tournament", "towel", "tower", "town", "townhouse", "township", "toy", "trace", "trachoma", "track",
"tracking", "tracksuit", "tract", "tractor", "trade", "trader", "trading", "tradition", "traditionalism", "traffic", "trafficker", "tragedy",
"trail", "trailer", "trailpatrol", "train", "trainer", "training", "trait", "tram", "tramp", "trance", "transaction", "transcript", "transfer",
"transformation", "transit", "transition", "translation", "transmission", "transom", "transparency", "transplantation", "transport",
"transportation", "trap", "trapdoor", "trapezium", "trapezoid", "trash", "travel", "traveler", "tray", "treasure", "treasury", "treat",
"treatment", "treaty", "tree", "trek", "trellis", "tremor", "trench", "trend", "triad", "trial", "triangle", "tribe", "tributary", "trick",
"trigger", "trigonometry", "trillion", "trim", "trinket", "trip", "tripod", "tritone", "triumph", "trolley", "trombone", "troop", "trooper",
"trophy", "trouble", "trousers", "trout", "trove", "trowel", "truck", "trumpet", "trunk", "trust", "trustee", "truth", "try", "tsunami",
"tub", "tuba", "tube", "tuber", "tug", "tugboat", "tuition", "tulip", "tumbler", "tummy", "tuna", "tune", "tunic", "tunnel",
"turban", "turf", "turkey", "turmeric", "turn", "turning", "turnip", "turnover", "turnstile", "turret", "turtle", "tusk", "tussle", "tutu",
"tuxedo", "tweet", "tweezers", "twig", "twilight", "twine", "twins", "twist", "twister", "twitter", "type", "typeface", "typewriter",
"typhoon", "ukulele", "ultimatum", "umbrella", "unblinking", "uncertainty", "uncle", "underclothes", "underestimate", "underground",
"underneath", "underpants", "underpass", "undershirt", "understanding", "understatement", "undertaker", "underwear", "underweight", "underwire",
"underwriting", "unemployment", "unibody", "uniform", "uniformity", "unique", "unit", "unity", "universe", "university", "update",
"upgrade", "uplift", "upper", "upstairs", "upward", "urge", "urgency", "urn", "usage", "use", "user", "usher", "usual", "utensil", "utilisation",
"utility", "utilization", "vacation", "vaccine", "vacuum", "vagrant", "valance", "valentine", "validate", "validity", "valley", "valuable",
"value", "vampire", "van", "vanadyl", "vane", "vanilla", "vanity", "variability", "variable", "variant", "variation", "variety", "vascular",
"vase", "vault", "vaulting", "veal", "vector", "vegetable", "vegetarian", "vegetarianism", "vegetation", "vehicle", "veil", "vein", "veldt",
"vellum", "velocity", "velodrome", "velvet", "vendor", "veneer", "vengeance", "venison", "venom", "venti", "venture", "venue", "veranda",
"verb", "verdict", "verification", "vermicelli", "vernacular", "verse", "version", "vertigo", "verve", "vessel", "vest", "vestment",
"vet", "veteran", "veterinarian", "veto", "viability", "vibe", "vibraphone", "vibration", "vibrissae", "vice", "vicinity", "victim",
"victory", "video", "view", "viewer", "vignette", "villa", "village", "vine", "vinegar", "vineyard", "vintage", "vintner", "vinyl", "viola",
"violation", "violence", "violet", "violin", "virginal", "virtue", "virus", "visa", "viscose", "vise", "vision", "visit", "visitor",
"visor", "vista", "visual", "vitality", "vitamin", "vitro", "vivo", "vixen", "vodka", "vogue", "voice", "void", "vol", "volatility",
"volcano", "volleyball", "volume", "volunteer", "volunteering", "vomit", "vote", "voter", "voting", "voyage", "vulture", "wad", "wafer",
"waffle", "wage", "wagon", "waist", "waistband", "wait", "waiter", "waiting", "waitress", "waiver", "wake", "walk", "walker", "walking",
"walkway", "wall", "wallaby", "wallet", "walnut", "walrus", "wampum", "wannabe", "want", "war", "warden", "wardrobe", "warfare", "warlock",
"warlord", "warming", "warmth", "warning", "warrant", "warren", "warrior", "wasabi", "wash", "washbasin", "washcloth", "washer",
"washtub", "wasp", "waste", "wastebasket", "wasting", "watch", "watcher", "watchmaker", "water", "waterbed", "watercress", "waterfall",
"waterfront", "watermelon", "waterskiing", "waterspout", "waterwheel", "wave", "waveform", "wax", "way", "weakness", "wealth", "weapon",
"wear", "weasel", "weather", "web", "webinar", "webmail", "webpage", "website", "wedding", "wedge", "weed", "weeder", "weedkiller", "week",
"weekend", "weekender", "weight", "weird", "welcome", "welfare", "well", "west", "western", "wetland", "wetsuit",
"whack", "whale", "wharf", "wheat", "wheel", "whelp", "whey", "whip", "whirlpool", "whirlwind", "whisker", "whiskey", "whisper", "whistle",
"white", "whole", "wholesale", "wholesaler", "whorl", "wick", "widget", "widow", "width", "wife", "wifi", "wild", "wildebeest", "wilderness",
"wildlife", "will", "willingness", "willow", "win", "wind", "windage", "window", "windscreen", "windshield", "wine", "winery",
"wing", "wingman", "wingtip", "wink", "winner", "winter", "wire", "wiretap", "wiring", "wisdom", "wiseguy", "wish", "wisteria", "wit",
"witch", "withdrawal", "witness", "wok", "wolf", "woman", "wombat", "wonder", "wont", "wood", "woodchuck", "woodland",
"woodshed", "woodwind", "wool", "woolens", "word", "wording", "work", "workbench", "worker", "workforce", "workhorse", "working", "workout",
"workplace", "workshop", "world", "worm", "worry", "worship", "worshiper", "worth", "wound", "wrap", "wraparound", "wrapper", "wrapping",
"wreck", "wrecker", "wren", "wrench", "wrestler", "wriggler", "wrinkle", "wrist", "writer", "writing", "wrong", "xylophone", "yacht",
"yahoo", "yak", "yam", "yang", "yard", "yarmulke", "yarn", "yawl", "year", "yeast", "yellow", "yellowjacket", "yesterday", "yew", "yin",
"yoga", "yogurt", "yoke", "yolk", "young", "youngster", "yourself", "youth", "yoyo", "yurt", "zampone", "zebra", "zebrafish", "zen",
"zephyr", "zero", "ziggurat", "zinc", "zipper", "zither", "zombie", "zone", "zoo", "zoologist", "zoology", "zucchini"
};


std::string_view obfuscateWord(std::string_view src, WordMap & obfuscate_map, WordSet & used_nouns, SipHash hash_func)
{
    /// Prevent using too many nouns
    if (obfuscate_map.size() * 2 > nouns.size())
        throw Exception("Too many unique identifiers in queries", ErrorCodes::TOO_MANY_TEMPORARY_COLUMNS);

    std::string_view & mapped = obfuscate_map[src];
    if (!mapped.empty())
        return mapped;

    hash_func.update(src.data(), src.size());
    std::string_view noun = nouns.begin()[hash_func.get64() % nouns.size()];

    /// Prevent collisions
    while (!used_nouns.insert(noun).second)
    {
        hash_func.update('\0');
        noun = nouns.begin()[hash_func.get64() % nouns.size()];
    }

    mapped = noun;
    return mapped;
}


void obfuscateIdentifier(std::string_view src, WriteBuffer & result, WordMap & obfuscate_map, WordSet & used_nouns, SipHash hash_func)
{
    /// Find words in form 'snake_case', 'CamelCase' or 'ALL_CAPS'.

    const char * src_pos = src.data();
    const char * src_end = src_pos + src.size();

    const char * word_begin = src_pos;
    bool word_has_alphanumerics = false;

    auto append_word = [&]
    {
        std::string_view word(word_begin, src_pos - word_begin);

        if (keep_words.count(word))
        {
            result.write(word.data(), word.size());
        }
        else
        {
            std::string_view obfuscated_word = obfuscateWord(word, obfuscate_map, used_nouns, hash_func);

            /// Match the style of source word.
            bool first_caps = !word.empty() && isUpperAlphaASCII(word[0]);
            bool all_caps = first_caps && word.size() >= 2 && isUpperAlphaASCII(word[1]);

            for (size_t i = 0, size = obfuscated_word.size(); i < size; ++i)
            {
                if (all_caps || (i == 0 && first_caps))
                    result.write(toUpperIfAlphaASCII(obfuscated_word[i]));
                else
                    result.write(obfuscated_word[i]);
            }
        }

        word_begin = src_pos;
        word_has_alphanumerics = false;
    };

    while (src_pos < src_end)
    {
        if (isAlphaNumericASCII(src_pos[0]))
            word_has_alphanumerics = true;

        if (word_has_alphanumerics && src_pos[0] == '_')
        {
            append_word();
            result.write('_');
            ++word_begin;
        }
        else if (word_has_alphanumerics && isUpperAlphaASCII(src_pos[0]) && isLowerAlphaASCII(src_pos[-1])) /// xX
        {
            append_word();
        }

        ++src_pos;
    }

    if (word_begin < src_pos)
        append_word();
}


void obfuscateLiteral(std::string_view src, WriteBuffer & result, SipHash hash_func)
{
    const char * src_pos = src.data();
    const char * src_end = src_pos + src.size();

    while (src_pos < src_end)
    {
        /// Date
        if (src_pos + strlen("0000-00-00") <= src_end
            && isNumericASCII(src_pos[0])
            && isNumericASCII(src_pos[1])
            && isNumericASCII(src_pos[2])
            && isNumericASCII(src_pos[3])
            && src_pos[4] == '-'
            && isNumericASCII(src_pos[5])
            && isNumericASCII(src_pos[6])
            && src_pos[7] == '-'
            && isNumericASCII(src_pos[8])
            && isNumericASCII(src_pos[9]))
        {
            DayNum date;
            ReadBufferFromMemory in(src_pos, strlen("0000-00-00"));
            readDateText(date, in);

            SipHash hash_func_date = hash_func;

            if (date != 0)
            {
                date += hash_func_date.get64() % 256;
            }

            writeDateText(date, result);
            src_pos += strlen("0000-00-00");

            /// DateTime
            if (src_pos + strlen(" 00:00:00") <= src_end
                && isNumericASCII(src_pos[1])
                && isNumericASCII(src_pos[2])
                && src_pos[3] == ':'
                && isNumericASCII(src_pos[4])
                && isNumericASCII(src_pos[5])
                && src_pos[6] == ':'
                && isNumericASCII(src_pos[7])
                && isNumericASCII(src_pos[8]))
            {
                result.write(src_pos[0]);

                hash_func_date.update(src_pos + 1, strlen("00:00:00"));

                uint64_t hash_value = hash_func_date.get64();
                uint32_t new_hour = hash_value % 24;
                hash_value /= 24;
                uint32_t new_minute = hash_value % 60;
                hash_value /= 60;
                uint32_t new_second = hash_value % 60;

                result.write('0' + (new_hour / 10));
                result.write('0' + (new_hour % 10));
                result.write(':');
                result.write('0' + (new_minute / 10));
                result.write('0' + (new_minute % 10));
                result.write(':');
                result.write('0' + (new_second / 10));
                result.write('0' + (new_second % 10));

                src_pos += strlen(" 00:00:00");
            }
        }
        else if (isNumericASCII(src_pos[0]))
        {
            /// Number
            if (src_pos[0] == '0' || src_pos[0] == '1')
            {
                /// Keep zero and one as is.
                result.write(src_pos[0]);
                ++src_pos;
            }
            else
            {
                ReadBufferFromMemory in(src_pos, src_end - src_pos);
                uint64_t num;
                readIntText(num, in);
                SipHash hash_func_num = hash_func;
                hash_func_num.update(src_pos, in.count());
                src_pos += in.count();

                /// Obfuscate number but keep it within same power of two range.

                uint64_t obfuscated = hash_func_num.get64();
                uint64_t log2 = bitScanReverse(num);

                obfuscated = (1ULL << log2) + obfuscated % (1ULL << log2);
                writeIntText(obfuscated, result);
            }
        }
        else if (src_pos + 1 < src_end
            && (src_pos[0] == 'e' || src_pos[0] == 'E')
            && (isNumericASCII(src_pos[1]) || (src_pos[1] == '-' && src_pos + 2 < src_end && isNumericASCII(src_pos[2]))))
        {
            /// Something like an exponent of floating point number. Keep it as is.
            /// But if it looks like a large number, overflow it into 16 bit.

            result.write(src_pos[0]);
            ++src_pos;

            ReadBufferFromMemory in(src_pos, src_end - src_pos);
            int16_t num;
            readIntText(num, in);
            writeIntText(num, result);
            src_pos += in.count();
        }
        else if (isAlphaASCII(src_pos[0]))
        {
            /// Alphabetial characters

            const char * alpha_end = src_pos + 1;
            while (alpha_end < src_end && isAlphaASCII(*alpha_end))
                ++alpha_end;

            hash_func.update(src_pos, alpha_end - src_pos);
            pcg64 rng(hash_func.get64());

            while (src_pos < alpha_end)
            {
                auto random = rng();
                if (isLowerAlphaASCII(*src_pos))
                    result.write('a' + random % 26);
                else
                    result.write('A' + random % 26);

                ++src_pos;
            }
        }
        else if (isASCII(src_pos[0]))
        {
            /// Punctuation, whitespace and control characters - keep as is.

            result.write(src_pos[0]);
            ++src_pos;
        }
        else if (src_pos[0] <= '\xBF')
        {
            /// Continuation of UTF-8 sequence.
            hash_func.update(src_pos[0]);
            uint64_t hash = hash_func.get64();

            char c = 0x80 + hash % (0xC0 - 0x80);
            result.write(c);

            ++src_pos;
        }
        else
        {
            /// Start of UTF-8 sequence.
            hash_func.update(src_pos[0]);
            uint64_t hash = hash_func.get64();

            if (src_pos[0] < '\xE0')
            {
                char c = 0xC0 + hash % 32;
                result.write(c);
            }
            else if (src_pos[0] < '\xF0')
            {
                char c = 0xE0 + hash % 16;
                result.write(c);
            }
            else
            {
                char c = 0xF0 + hash % 8;
                result.write(c);
            }

            ++src_pos;
        }
    }
}

}


void obfuscateQueries(
    std::string_view src,
    WriteBuffer & result,
    WordMap & obfuscate_map,
    WordSet & used_nouns,
    SipHash hash_func,
    KnownIdentifierFunc known_identifier_func)
{
    Lexer lexer(src.data(), src.data() + src.size());
    while (true)
    {
        Token token = lexer.nextToken();
        std::string_view whole_token(token.begin, token.size());

        if (token.isEnd())
            break;

        if (token.type == TokenType::BareWord)
        {
            std::string whole_token_uppercase(whole_token);
            Poco::toUpperInPlace(whole_token_uppercase);

            if (keywords.count(whole_token_uppercase)
                || known_identifier_func(whole_token))
            {
                /// Keep keywords as is.
                result.write(token.begin, token.size());
            }
            else
            {
                /// Obfuscate identifiers
                obfuscateIdentifier(whole_token, result, obfuscate_map, used_nouns, hash_func);
            }
        }
        else if (token.type == TokenType::QuotedIdentifier)
        {
            assert(token.size() >= 2);

            /// Write quotes and the obfuscated content inside.
            result.write(*token.begin);

            /// If it is long, just replace it with hash. Long identifiers in queries are usually auto-generated.
            if (token.size() > 32)
                writeIntText(sipHash64(token.begin + 1, token.size() - 2), result);
            else
                obfuscateIdentifier({token.begin + 1, token.size() - 2}, result, obfuscate_map, used_nouns, hash_func);

            result.write(token.end[-1]);
        }
        else if (token.type == TokenType::Number)
        {
            obfuscateLiteral(whole_token, result, hash_func);
        }
        else if (token.type == TokenType::StringLiteral)
        {
            assert(token.size() >= 2);

            result.write(*token.begin);
            obfuscateLiteral({token.begin + 1, token.size() - 2}, result, hash_func);
            result.write(token.end[-1]);
        }
        else if (token.type == TokenType::Comment)
        {
            /// Skip comments - they may contain confidential info.
        }
        else
        {
            /// Everything else is kept as is.
            result.write(token.begin, token.size());
        }
    }
}

}

