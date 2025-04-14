import React, { useState, useEffect } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Paper from '@mui/material/Paper';
import Checkbox from '@mui/material/Checkbox';
import FormControlLabel from '@mui/material/FormControlLabel';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Button from '@mui/material/Button';
import ButtonGroup from '@mui/material/ButtonGroup';
import TextField from '@mui/material/TextField';
import Chip from '@mui/material/Chip';
import Alert from '@mui/material/Alert';

const SchemaSelector = ({ columns, onColumnsSelected, title = 'Select Columns' }) => {
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [filter, setFilter] = useState('');
  const [filteredColumns, setFilteredColumns] = useState([]);

  // Initialize selected columns from props
  useEffect(() => {
    if (columns) {
      const initialSelected = columns
        .filter((col) => col.selected)
        .map((col) => col.name);
      setSelectedColumns(initialSelected);
      setFilteredColumns(columns);
    }
  }, [columns]);

  // Filter columns when filter text changes
  useEffect(() => {
    if (columns) {
      const filtered = columns.filter((col) =>
        col.name.toLowerCase().includes(filter.toLowerCase())
      );
      setFilteredColumns(filtered);
    }
  }, [filter, columns]);

  const handleToggleColumn = (columnName) => {
    setSelectedColumns((prevSelected) => {
      if (prevSelected.includes(columnName)) {
        return prevSelected.filter((name) => name !== columnName);
      } else {
        return [...prevSelected, columnName];
      }
    });
  };

  const handleSelectAll = () => {
    setSelectedColumns(filteredColumns.map((col) => col.name));
  };

  const handleDeselectAll = () => {
    setSelectedColumns([]);
  };

  const handleApply = () => {
    if (onColumnsSelected) {
      onColumnsSelected(selectedColumns);
    }
  };

  const handleFilterChange = (e) => {
    setFilter(e.target.value);
  };

  if (!columns || columns.length === 0) {
    return (
      <Paper elevation={2} sx={{ p: 3, mb: 3 }}>
        <Alert severity="info">No columns available for selection.</Alert>
      </Paper>
    );
  }

  return (
    <Paper elevation={2} sx={{ p: 3, mb: 3 }}>
      <Typography variant="h6" gutterBottom>
        {title}
      </Typography>

      <Box sx={{ mb: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <ButtonGroup variant="outlined" size="small">
          <Button onClick={handleSelectAll}>Select All</Button>
          <Button onClick={handleDeselectAll}>Deselect All</Button>
        </ButtonGroup>

        <TextField
          size="small"
          label="Filter Columns"
          variant="outlined"
          value={filter}
          onChange={handleFilterChange}
        />
      </Box>

      <Box sx={{ mb: 2 }}>
        <Typography variant="body2" color="text.secondary" gutterBottom>
          Selected: {selectedColumns.length} of {columns.length} columns
        </Typography>
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
          {selectedColumns.slice(0, 5).map((name) => (
            <Chip
              key={name}
              label={name}
              size="small"
              onDelete={() => handleToggleColumn(name)}
            />
          ))}
          {selectedColumns.length > 5 && (
            <Chip label={`+${selectedColumns.length - 5} more`} size="small" />
          )}
        </Box>
      </Box>

      <TableContainer sx={{ maxHeight: 400 }}>
        <Table stickyHeader size="small">
          <TableHead>
            <TableRow>
              <TableCell padding="checkbox">Select</TableCell>
              <TableCell>Column Name</TableCell>
              <TableCell>Type</TableCell>
              <TableCell align="center">Nullable</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {filteredColumns.map((column) => (
              <TableRow key={column.name} hover>
                <TableCell padding="checkbox">
                  <Checkbox
                    checked={selectedColumns.includes(column.name)}
                    onChange={() => handleToggleColumn(column.name)}
                  />
                </TableCell>
                <TableCell component="th" scope="row">
                  {column.name}
                </TableCell>
                <TableCell>{column.type}</TableCell>
                <TableCell align="center">
                  {column.is_nullable ? 'Yes' : 'No'}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <Button
        variant="contained"
        onClick={handleApply}
        disabled={selectedColumns.length === 0}
        sx={{ mt: 2 }}
        fullWidth
      >
        Apply Selection
      </Button>
    </Paper>
  );
};

export default SchemaSelector; 