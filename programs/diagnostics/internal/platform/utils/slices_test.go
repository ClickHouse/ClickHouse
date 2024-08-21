package utils_test

import (
	"testing"

	"github.com/ClickHouse/ClickHouse/programs/diagnostics/internal/platform/utils"
	"github.com/stretchr/testify/require"
)

func TestIntersection(t *testing.T) {
	t.Run("can perform intersection", func(t *testing.T) {
		setA := []string{"A", "b", "C", "D", "E"}
		setB := []string{"A", "B", "F", "C", "G"}
		setC := utils.Intersection(setA, setB)
		require.Len(t, setC, 2)
		require.ElementsMatch(t, []string{"A", "C"}, setC)
	})
}

func TestDistinct(t *testing.T) {
	t.Run("can perform distinct", func(t *testing.T) {
		setA := []string{"A", "b", "C", "D", "E"}
		setB := []string{"A", "B", "F", "C", "G"}
		setC := utils.Distinct(setA, setB)
		require.Len(t, setC, 3)
		require.ElementsMatch(t, []string{"b", "D", "E"}, setC)
	})

	t.Run("can perform distinct on empty", func(t *testing.T) {
		setA := []string{"A", "b", "C", "D", "E"}
		var setB []string
		setC := utils.Distinct(setA, setB)
		require.Len(t, setC, 5)
		require.ElementsMatch(t, []string{"A", "b", "C", "D", "E"}, setC)
	})
}

func TestContains(t *testing.T) {
	t.Run("can perform contains", func(t *testing.T) {
		setA := []string{"A", "b", "C", "D", "E"}
		require.True(t, utils.Contains(setA, "A"))
		require.True(t, utils.Contains(setA, "b"))
		require.True(t, utils.Contains(setA, "C"))
		require.True(t, utils.Contains(setA, "D"))
		require.True(t, utils.Contains(setA, "E"))
		require.False(t, utils.Contains(setA, "B"))
	})
}

func TestUnique(t *testing.T) {

	t.Run("can perform unique", func(t *testing.T) {
		setA := []string{"A", "b", "D", "D", "E", "E", "A"}
		setC := utils.Unique(setA)
		require.Len(t, setC, 4)
		require.ElementsMatch(t, []string{"A", "b", "D", "E"}, setC)
	})

	t.Run("can perform unique on empty", func(t *testing.T) {
		var setA []string
		setC := utils.Unique(setA)
		require.Len(t, setC, 0)
	})
}
