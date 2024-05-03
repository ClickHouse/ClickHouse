package utils

// Intersection of elements in s1 and s2
func Intersection(s1, s2 []string) (inter []string) {
	hash := make(map[string]bool)
	for _, e := range s1 {
		hash[e] = false
	}
	for _, e := range s2 {
		// If elements present in the hashmap then append intersection list.
		if val, ok := hash[e]; ok {
			if !val {
				// only add once
				inter = append(inter, e)
				hash[e] = true
			}
		}
	}
	return inter
}

// Distinct returns elements in s1, not in s2
func Distinct(s1, s2 []string) (distinct []string) {
	hash := make(map[string]bool)
	for _, e := range s2 {
		hash[e] = true
	}
	for _, e := range s1 {
		if _, ok := hash[e]; !ok {
			distinct = append(distinct, e)
		}
	}
	return distinct
}

// Unique func Unique(s1 []string) (unique []string) returns unique elements in s1
func Unique(s1 []string) (unique []string) {
	hash := make(map[string]bool)
	for _, e := range s1 {
		if _, ok := hash[e]; !ok {
			unique = append(unique, e)
		}
		hash[e] = true
	}
	return unique
}

func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func IndexOf(s []string, e string) int {
	for i, a := range s {
		if a == e {
			return i
		}
	}
	return -1
}

func Remove(slice []interface{}, s int) []interface{} {
	return append(slice[:s], slice[s+1:]...)
}
