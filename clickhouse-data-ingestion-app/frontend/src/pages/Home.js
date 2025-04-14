import React from 'react';
import { Link } from 'react-router-dom';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import CardActions from '@mui/material/CardActions';
import Button from '@mui/material/Button';
import Grid from '@mui/material/Grid';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import StorageIcon from '@mui/icons-material/Storage';
import DescriptionIcon from '@mui/icons-material/Description';

const Home = () => {
  return (
    <>
      <Box sx={{ textAlign: 'center', mb: 6 }}>
        <Typography variant="h3" component="h1" gutterBottom>
          ClickHouse Data Ingestion Tool
        </Typography>
        <Typography variant="h6" color="text.secondary" paragraph>
          A bidirectional data transfer tool for ClickHouse databases and flat files
        </Typography>
      </Box>

      <Grid container spacing={4} sx={{ mb: 4 }}>
        <Grid item xs={12} md={6}>
          <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <StorageIcon fontSize="large" color="primary" sx={{ mr: 2 }} />
                <ArrowForwardIcon sx={{ mx: 1 }} />
                <DescriptionIcon fontSize="large" color="secondary" />
              </Box>
              <Typography gutterBottom variant="h5" component="h2">
                ClickHouse to File
              </Typography>
              <Typography>
                Export data from ClickHouse database tables to flat files like CSV. Connect to your
                database, select tables and columns, and save the data to a file.
              </Typography>
            </CardContent>
            <CardActions>
              <Button 
                component={Link} 
                to="/clickhouse-to-file" 
                size="large" 
                variant="contained" 
                fullWidth
                endIcon={<ArrowForwardIcon />}
              >
                Get Started
              </Button>
            </CardActions>
          </Card>
        </Grid>
        
        <Grid item xs={12} md={6}>
          <Card sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
            <CardContent sx={{ flexGrow: 1 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                <DescriptionIcon fontSize="large" color="secondary" sx={{ mr: 2 }} />
                <ArrowForwardIcon sx={{ mx: 1 }} />
                <StorageIcon fontSize="large" color="primary" />
              </Box>
              <Typography gutterBottom variant="h5" component="h2">
                File to ClickHouse
              </Typography>
              <Typography>
                Import data from flat files into ClickHouse database tables. Select your file,
                configure the schema, and import data into a new or existing table.
              </Typography>
            </CardContent>
            <CardActions>
              <Button 
                component={Link} 
                to="/file-to-clickhouse" 
                size="large" 
                variant="contained" 
                fullWidth
                endIcon={<ArrowForwardIcon />}
              >
                Get Started
              </Button>
            </CardActions>
          </Card>
        </Grid>
      </Grid>

      <Box sx={{ mt: 6, p: 4, bgcolor: 'background.paper', borderRadius: 2 }}>
        <Typography variant="h5" gutterBottom>
          Features
        </Typography>
        <Grid container spacing={2}>
          <Grid item xs={12} sm={6}>
            <Typography variant="subtitle1" gutterBottom>
              • Connect to ClickHouse with JWT authentication
            </Typography>
            <Typography variant="subtitle1" gutterBottom>
              • Configure flat files with custom delimiters
            </Typography>
            <Typography variant="subtitle1" gutterBottom>
              • Automatic schema discovery
            </Typography>
          </Grid>
          <Grid item xs={12} sm={6}>
            <Typography variant="subtitle1" gutterBottom>
              • Data preview for both sources
            </Typography>
            <Typography variant="subtitle1" gutterBottom>
              • Progress tracking for data transfers
            </Typography>
            <Typography variant="subtitle1" gutterBottom>
              • Create new tables or use existing ones
            </Typography>
          </Grid>
        </Grid>
      </Box>
    </>
  );
};

export default Home; 